package main

import (
 "log"
 "fmt"
 "bytes"
 "flag"
 "os"
 "os/exec"
 "errors"
 "strings"
 "path/filepath"
 "github.com/aws/aws-sdk-go/aws"
 "github.com/aws/aws-sdk-go/aws/session"
 "github.com/aws/aws-sdk-go/service/s3"
 "github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var LEPTON bool
var LEPTON_BIN string
var AWS_CRED_LOCATION string
var S3 bool
var S3_BUCKET_NAME string
var S3_PREFIX string
var DIR string
var s3sess *session.Session
var uploader *s3manager.Uploader
var s3svc *s3.S3
var s3UpQueue []string
var s3BucketContents []*s3.Object

func PathExists(path string) (bool,error) {
 _, err := os.Stat(path)
 if err == nil { return true, nil }
 if os.IsNotExist(err) { return false, nil }
 return true, err
}

func CheckLeptonBin() bool {
 _, err := exec.LookPath(LEPTON_BIN)
 if err != nil {
  e, _ := PathExists(LEPTON_BIN)
  if !e { return false }
 }
 return true
}

func CheckLeptonDir(path string) error {
 exists, err := PathExists(path)
 if err != nil {
  log.Println("Dodgy path:", err)
  return filepath.SkipDir
 }
 if !exists {
  err = os.Mkdir(path, 0777)
  if err != nil {
   log.Println("Cannot create lepton dir: ", err)
   return filepath.SkipDir
  }
 }
 return nil
}

func CreateLepton(dir string, name string, dest string) (string, error) {
 newfile := filepath.Join(dest, name + ".lep")
 if e, err := PathExists(newfile); err != nil {
  return "", errors.New(fmt.Sprint("Lepton directory error for file %s", newfile))
 } else if !e {
  cmd := exec.Command(LEPTON_BIN, "-allowprogressive", filepath.Join(dir, name), newfile)
  var out bytes.Buffer
  cmd.Stdout = &out
  err := cmd.Run()
  if err != nil {
   //log.Println("Failed to lepton ", name, err)
   return "", errors.New(fmt.Sprint("Failed to compress file %s", newfile))
  } else { log.Println(out.String()) }
 } else { log.Println("Skipping", newfile, " already exists") }
 return newfile, nil
}

func WalkerFunc(path string, info os.FileInfo, err error) error {
 var toUpload string
 if err != nil { return err }
 if !info.IsDir() {
  n := strings.ToLower(info.Name())
  isJpg := strings.Contains(n, ".jpg")
  if !isJpg { isJpg = strings.Contains(n, ".jpeg") }
  if isJpg {
   currentdir, filename := filepath.Split(path)
   if LEPTON {
    leptondir := filepath.Join(currentdir, ".leptons")
    if err = CheckLeptonDir(leptondir); err != nil { return err }
    toUpload, err = CreateLepton(currentdir, filename, leptondir)
    if err != nil {
     log.Println(err)
     return filepath.SkipDir
    }
   } else {
    toUpload = path
   }
   if S3 {
    u, err := s3CheckFile(toUpload)
    if err != nil {
     log.Println(err)
     return filepath.SkipDir
    }
    if !u { s3UpQueue = append(s3UpQueue, toUpload); log.Println(path, "Added to queue") } // if S3 not up to date, add the file to upload queue
   }
  }
 } else {
  if strings.Contains(info.Name(), ".leptons") { return filepath.SkipDir }
 }
 return nil
}

func processUploadQueue(q []string) {
 for _, f := range q {
  s3UploadFile(f)
 }
}

func s3UploadFile(f string) {
 file, err := os.Open(f)
 if err != nil {
  log.Println("Failed opening file", f, err)
  return
 }
 defer file.Close()
 k, err := filepath.Rel(DIR, f)
 if err != nil {
  log.Println("Couldn't make rel:", err)
  return
 }
 k = strings.Replace(k, "/.leptons", "", 1) // take out the /.leptons from the path as we dont want that directory on AWS
 if len(S3_PREFIX) > 0 { k = filepath.Join(S3_PREFIX, k) }
 result, err := uploader.Upload(&s3manager.UploadInput{
  Bucket: &S3_BUCKET_NAME,
  Key: aws.String(k),
  Body: file,
  ServerSideEncryption: aws.String("AES256"),
 })
 if err != nil {
  log.Println("Failed to upload", f, err)
  return
 }
 log.Println("Uploaded", f, result.Location)
}

func s3GetBucketContents() error {
 params := &s3.ListObjectsV2Input{
  Bucket: aws.String(S3_BUCKET_NAME),
 }
 pageNum := 0
 err := s3svc.ListObjectsV2Pages(params,
  func(page *s3.ListObjectsV2Output, lastPage bool) bool {
   pageNum++
   fmt.Println("Loaded bucket page", pageNum)
   s3BucketContents = append(s3BucketContents, page.Contents...)
   return !lastPage
  })
 if err != nil { return err }
 log.Println(len(s3BucketContents), " items loaded from bucket")
 return nil
}

func s3CheckFile(p string) (bool, error) {
 info, err := os.Stat(p)
 if err != nil {
  log.Println(err)
  return false, err
 }
 k, err := filepath.Rel(DIR, p) // get the relative path to match what the key on AWS will be
 if err != nil {
  log.Println("Couldn't make rel:", err)
  return false, err
 }
 k = strings.Replace(k, "/.leptons", "", 1) // take out the /.leptons from the path as we dont use that directory on AWS
 for _, o := range s3BucketContents {
  if *o.Key == k { // key is a *string, not a string!!
   t := *o.LastModified
   if t.After(info.ModTime()) {
    log.Println(k, "is up to date")
    return true, nil
   }
   break
  }
 }
 return false, nil
}

func main() {
 flag.BoolVar(&LEPTON, "lepton", false, "Compress Jpeg images using Lepton")
 flag.StringVar(&LEPTON_BIN, "lepton-bin", "lepton", "Specify location of the lepton binary, if not installed")
 flag.StringVar(&AWS_CRED_LOCATION, "awscreds", "~/.aws/credentials", "Specify AWS credentials to use")
 flag.BoolVar(&S3, "s3upload", false, "Upload images to Amazon S3")
 flag.StringVar(&S3_BUCKET_NAME, "s3bucket", "", "Specify AWS bucket to upload into")
 flag.StringVar(&S3_PREFIX, "s3prefix", "", "Specify the prefix for S3 upload (folder/file-prefix)")
 flag.Parse()
 if flag.NArg() < 1 { DIR, _ = os.Getwd() } else {
  DIR = flag.Arg(0)
  os.Chdir(DIR)
 }
 log.Println("Walking through", DIR)
 log.Println("Lepton compression enabled:", LEPTON)
 if LEPTON {
  if !CheckLeptonBin() { log.Fatal("Lepton binary not found") }
 }
 var err error
 if S3 {
  log.Println(len(S3_BUCKET_NAME))
  if len(S3_BUCKET_NAME) < 1 { log.Fatal("Please supply a bucket name to use the S3 upload feature") }
  s3sess, err = session.NewSession(&aws.Config{Region: aws.String("eu-west-1")})
  if err != nil { log.Fatal(err) }
  s3svc = s3.New(s3sess)
  if err = s3GetBucketContents(); err != nil {
   log.Fatal(err)
  }
 }
 if err = filepath.Walk(DIR, WalkerFunc); err != nil { log.Fatal(err) }
 if S3 {
  uploader = s3manager.NewUploader(s3sess)
  processUploadQueue(s3UpQueue)
 }
}
