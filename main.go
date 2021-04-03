package main

import (
	"database/sql"
	"fmt"
	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"github.com/satori/go.uuid"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

var (
	readChan chan string
	writeChan chan string
	DB *sql.DB
	TICK_SIGN = "SIGN-TIMER-TICK"
)

const (
	USERNAME = "root"
	PASSWORD = "root"
	NETWORK  = "tcp"
	SERVER   = "localhost"
	PORT     = 3306
	DATABASE = "test"
)

func main() {
	readChan = make(chan string)
	writeChan = make(chan string)
	go Process("process1")
	//go Process("process2")
	go Write("write1")
	//go Write("write2")
	go func() {
		tick := time.Tick(10*time.Second)
		for range tick {
			println("10s")
			readChan <- TICK_SIGN
		}
	}()
	DB = connectMySQL()

	r := gin.Default()
	r.Use(func(context *gin.Context) {
		tracerId := context.Request.Header.Get("tracerId")
		if tracerId == ""{
			tracerId = uuid.NewV4().String()
		}
		context.Set("tracerId",tracerId)
		context.Writer.Header().Set("tracerId",tracerId)
		context.Next()
	})
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(200,gin.H{
			"message":"pong",
		})
	})
	r.POST("/test",TestHandler)
	r.Run()
}

func TestHandler(c *gin.Context)  {
	data,_:= ioutil.ReadAll(c.Request.Body)
	//spew.Dump(c.Get("tracerId"))
	trackerID,_ := c.Get("tracerId")
	uid := uuid.NewV4().String()
	readChan <- trackerID.(string)+"@"+uid
	//readChan <- string(data)+"-"+time.Now().String()
	c.JSON(http.StatusOK, map[string]interface{}{
		"code":http.StatusOK,
		"msg":string(data),
		"tracker_id": trackerID.(string),
		"uuid":uid,
	})
}

func Process(name string)  {
	fmt.Println(name,"start process")
	dataSize := 0
	datas:=[]string{}
	for v:=  range readChan {
		if v != TICK_SIGN {
			datas = append(datas,v+"#"+strconv.Itoa(rand.Intn(1000)))
			dataSize += int(unsafe.Sizeof(v))+len(v)
		}
		if (dataSize >= 500 || v == TICK_SIGN) && len(datas)>0{
			for _,str := range datas {
				writeChan <- str
			}
			//writeChan <- datas
			dataSize = 0
			datas = datas[:0]
		}
		fmt.Println(name,"-from-process:",v,", data size:",dataSize)
	}
}

func Write( name string)  {
	fmt.Println(name,"start write")
	for v := range writeChan {
		batchUUID := uuid.NewV4().String()
		//for _,rowdata := range v {
			splits := strings.Split(v,"@")
			row := []interface{}{name,time.Now().String(),batchUUID,splits[0],splits[1]}
			insertData(row)
		//}
		fmt.Println(name,"-from-write:",v)
	}
}

func connectMySQL() *sql.DB  {
	dsn := fmt.Sprintf("%s:%s@%s(%s:%d)/%s",USERNAME,PASSWORD,NETWORK,SERVER,PORT,DATABASE)
	DB,err := sql.Open("mysql",dsn)
	if err != nil{
		fmt.Printf("Open mysql failed,err:%v\n",err)
		return nil
	}
	if err =DB.Ping(); err != nil {
		fmt.Println(err)
	}else{
		fmt.Println("connected to mysql successful!")
	}
	DB.SetConnMaxLifetime(100*time.Second)  //最大连接周期，超过时间的连接就close
	DB.SetMaxOpenConns(100)
	DB.SetMaxIdleConns(16)
	return DB
}

func createTable()  {
	sql := `
			CREATE TABLE test (
					id INT ( 11 ) NOT NULL AUTO_INCREMENT,
					name VARCHAR ( 1000 ) DEFAULT NULL,
					app_date VARCHAR ( 200 ) DEFAULT NULL,
					batch_uuid VARCHAR ( 200 ) DEFAULT NULL,
					tracer_id VARCHAR ( 200 ) DEFAULT NULL,
					uuid VARCHAR ( 200 ) DEFAULT NULL,
					created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
					PRIMARY KEY ( id )
				) ENGINE = INNODB AUTO_INCREMENT = 0 DEFAULT CHARSET = utf8mb4;
	`
	_,err := DB.Exec(sql)
	if err != nil{
		fmt.Printf("Insert failed,err:%v",err)
		return
	}

}

func insertData(data []interface{})  {
	//row := []interface{}{"aa","2021-04-03 13:28:19.1968491 +0800 CST m=+1312.484802101",22}
	result,err := DB.Exec("insert into test(name,app_date,batch_uuid,tracer_id,uuid) values(?,?,?,?,?)",data...)
	if err != nil{
		fmt.Printf("Insert failed,err:%v",err)
		return
	}
	lastInsertID,err := result.LastInsertId()  //插入数据的主键id
	if err != nil {
		fmt.Printf("Get lastInsertID failed,err:%v",err)
		return
	}
	fmt.Println("LastInsertID:",lastInsertID)
	rowsaffected,err := result.RowsAffected()  //影响行数
	if err != nil {
		fmt.Printf("Get RowsAffected failed,err:%v",err)
		return
	}
	fmt.Println("RowsAffected:",rowsaffected)
}