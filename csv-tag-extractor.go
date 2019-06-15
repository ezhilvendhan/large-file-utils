package main

import (
  "fmt"
  "os"
  "log"
  "strconv"
  "strings"
  "runtime"
  "path"
  "path/filepath"
  "io/ioutil"
  "bufio"
  "time"
)

const Out = "out"
var tags = make(map[string]bool)

func main() {
  start := time.Now()
  log.Printf("Start Time %s", start)
  maxIdx := 10
  mode := int(0777)
  cwd := getCWD()
  _ = os.Mkdir(filepath.Join(cwd, Out), os.FileMode(mode))

  for i := 1; i <= maxIdx; i++ {
    folderPath := filepath.Join(cwd, strconv.Itoa(i))
    if _, err := os.Stat(folderPath); err == nil {
      if os.IsNotExist(err) {
        fmt.Printf("stat error [%v]\n", err)
        break
      }
      fmt.Printf(folderPath+"\n")
      wErr := filepath.Walk(folderPath, func(path string, info os.FileInfo, wErr error) error {
        if info.IsDir() {
          return nil
        }
        if filepath.Ext(path) == ".csv" {
          tags = make(map[string]bool)
          ProcessFile(path)
        }
        return nil
      })
      if wErr != nil {
        fmt.Printf("walk error [%v]\n", wErr)
      }
    }
  }
  elapsed := time.Since(start)
  log.Printf("Time taken %s", elapsed)
}

func getCWD() (_path string){
  _, filename, _, ok := runtime.Caller(0)
  if !ok {
      panic("No caller information")
  }
  _path = path.Dir(filename)
  return 
}

func ProcessFile(_file string) {
  file, err := os.Open(_file)
  if err != nil {
      log.Fatal(err)
  }
  defer file.Close()

  dataToWrite := ""
  prevTag := ""
  scanner := bufio.NewScanner(file)
  for scanner.Scan() {
    line := scanner.Text()
    _tag := strings.Split(line, ",")[0]
    line = line+"\n"
    if tags[_tag] == true {
      dataToWrite += line
    } else {
      tags[_tag] = true
      if prevTag != "" {
        _tagFileName := filepath.Join(getCWD(), "out", prevTag+".csv")
        if(!Exists(_tagFileName)) {
          _err := ioutil.WriteFile(_tagFileName, []byte(dataToWrite), 0777)
          if _err != nil {
            fmt.Printf("Unable to write file: %v", _err)
          }
        } else {
          AppendFile(_tagFileName, dataToWrite)
        }
      }
      dataToWrite = line
      prevTag = _tag
    }
  }

  if err := scanner.Err(); err != nil {
      log.Fatal(err)
  }

  _tagFileName := filepath.Join(getCWD(), "out", prevTag+".csv")
  if(!Exists(_tagFileName)) {
    _err := ioutil.WriteFile(_tagFileName, []byte(dataToWrite), 0777)
    if _err != nil {
      fmt.Printf("Unable to write file: %v", _err)
    }
  } else {
    AppendFile(_tagFileName, dataToWrite)
  }
}

func Exists(name string) bool {
  if _, err := os.Stat(name); err != nil {
      if os.IsNotExist(err) {
          return false
      }
  }
  return true
}

func AppendFile(fileName string, line string) {     
  file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_APPEND, 0644)
  if err != nil {
    log.Fatalf("failed opening file: %s", err)
  }
  defer file.Close()

  _, err = file.WriteString(line)
  if err != nil {
      log.Fatalf("failed writing to file: %s", err)
  }
}