### GeoMesa

#### 1.导入数据到geomesa-hbase

##### 1.1编辑特征文件与转换文件

- 特征文件phonedata.sft

  ```
  geomesa = {
    sfts = {
      example = {
        type-name = "example"
        attributes = [
          { name = "phoneid", type = "String", index = true }
          { name = "double_0", type = "Double", index = false }
          { name = "double_1", type = "Double", index = false }
          { name = "time", type = "Date",   index = false }
          { name = "geom", type = "Point",  index = true,srid = 4326,default = true }
     ]
      }
    }
  }
  ```

- 转换文件phonedata.convert 

  ```
  geomesa.converters.example = {
  geomesa.converters.example = {
      "fields" : [
          {
              "name" : "phoneid",
              "transform" : "toString($1)"
          },
          {
              "name" : "double_0",
              "transform" : "toDouble($2)"
          },
          {
              "name" : "double_1",
              "transform" : "toDouble($3)"
          },
          {
              "name" : "geom",
              "transform" : "point($double_0,$double_1)"
          },
          {
              "name" : "time",
              "transform" : "isoDateTime($4)"
          },
      ],
      "format" : "CSV",
      "id-field" : "md5(string2bytes($0))",
      "options" : {
          "encoding" : "UTF-8",
          "error-mode" : "skip-bad-records",
          "parse-mode" : "incremental",
          "validators" : [
              "index"
          ]
      },
      "type" : "delimited-text"
  }
  ```

##### 1.2在导入数据前需要对数据的时间列进行处理（使用的是spark处理，若数据已经处理过，此步骤可以省略）

- 读入数据

  ```
  val dfraw = spark.read.csv("/data/ZQ/geomesa/data/geotest/phone.csv")
  ```

  ![image-20220623140737824](md_img/geomesa%E6%9C%9F%E6%9C%AB%E5%A4%A7%E4%BD%9C%E4%B8%9A2.img/image-20220623140737824.png)

- 定义时间处理函数

  ```
  def replace(time:String):String = {
       val str = "2022-06-06T"
       val nyr = str.concat(time)
       nyr
       }
  ```

  ```
  val time_replace = udf(replace _)
  ```

- 使用函数转换时间，删除不需要的列并进行存储

  ```
  val df = dfraw.withColumn("nyr",time_replace(dfraw("_c3")))
  
  val df01 = df.drop("_c1")
  ```

  ![image-20220623140817879](md_img/geomesa%E6%9C%9F%E6%9C%AB%E5%A4%A7%E4%BD%9C%E4%B8%9A2.img/image-20220623140817879.png)

  ```
  val df02= df01.rdd.map(x => x(0) + "," + x(1) + "," + x(2) + "," + x(3))
  
  df02.coalesce(1).saveAsTextFile(s"/data/ZQ/geomesa/data/phone.csv")
  ```

##### 1.3使用命令行将数据导入geomesa-hbase（在此路径下执行：/home/hadoop/geomesa-hbase_2.11-3.4.0）

```
geomesa-hbase ingest --catalog qimo \
	--feature-name phone01 \
	--input-format csv \
	-C conf/phonedata.convert \
	-s conf/phonedata.sft \
	"data/phone.csv"
```

![image-20220606202802822](md_img/geomesa%E6%9C%9F%E6%9C%AB%E5%A4%A7%E4%BD%9C%E4%B8%9A2.img/image-20220606202802822.png)

在hbase中查看

![image-20220623141024734](md_img/geomesa%E6%9C%9F%E6%9C%AB%E5%A4%A7%E4%BD%9C%E4%B8%9A2.img/image-20220623141024734.png)

#### 2.geomesa-hbase与hive、spark查询性能对比

##### 2.1geomesa-hbase查询

- 时空查询

  ```
  bin/geomesa-hbase export -c qimo -f phone01 -q "time = '2022-06-06T22:58:48' && geom = 'POINT (113.895246 22.775922)'"
  ```

  ```
  114.011875 22.6715972
  bin/geomesa-hbase export -c qimo -f phone01 -q "time = '2022-06-06T22:58:48' && geom = 'POINT (114.011875 22.6715972)'"
  ```

  ![image-20220618215823991](md_img/geomesa%E6%9C%9F%E6%9C%AB%E5%A4%A7%E4%BD%9C%E4%B8%9A2.img/image-20220618215823991.png)

  ![image-20220625190127929](md_img/geomesa%E6%9C%9F%E6%9C%AB%E5%A4%A7%E4%BD%9C%E4%B8%9A2.img/image-20220625190127929.png)

- 时间查询

  ```
  bin/geomesa-hbase export -c qimo -f phone01 -q "time = '2022-06-06T22:58:48'"
  ```

  ![image-20220625163335507](md_img/geomesa%E6%9C%9F%E6%9C%AB%E5%A4%A7%E4%BD%9C%E4%B8%9A2.img/image-20220625163335507.png)

- 空间查询

  ```
  bin/geomesa-hbase export -c qimo -f phone01 -q "geom = 'POINT (113.895246 22.775922)'"
  ```

  ```
  bin/geomesa-hbase export -c qimo -f phone01 -q "geom = 'POINT (114.011875 22.6715972)'"
  ```

  ![image-20220618220510290](md_img/geomesa%E6%9C%9F%E6%9C%AB%E5%A4%A7%E4%BD%9C%E4%B8%9A2.img/image-20220618220510290-16559649503891.png)

##### 2.2hive查询

导入数据

```
create table phone01(id int,time string,lng double,lat double,datetime string) row format delimited fields terminated by ',';

load data inpath '/data/ZQ/geomesa/data/phone.csv/phone.csv' into table qimo.phone01;
```

![image-20220618215221132](md_img/geomesa%E6%9C%9F%E6%9C%AB%E5%A4%A7%E4%BD%9C%E4%B8%9A2.img/image-20220618215221132.png)

- 时空查询

  ```
  select * from phone01 where time = '2022-06-06T22:58:48' and lng = 113.895246 and lat = 22.775922;
  ```

  ![image-20220618215741467](md_img/geomesa%E6%9C%9F%E6%9C%AB%E5%A4%A7%E4%BD%9C%E4%B8%9A2.img/image-20220618215741467.png)

- 时间查询

  ```
  select * from phone01 where datetime = '2022-06-06T22:58:48';
  ```

  ![image-20220618220329110](md_img/geomesa%E6%9C%9F%E6%9C%AB%E5%A4%A7%E4%BD%9C%E4%B8%9A2.img/image-20220618220329110.png)

- 空间查询

  ```
  select * from phone01 where lng = 113.895246 and lat = 22.775922;
  ```

  ![image-20220618220622856](md_img/geomesa%E6%9C%9F%E6%9C%AB%E5%A4%A7%E4%BD%9C%E4%B8%9A2.img/image-20220618220622856.png)

- 一般属性

  ```
  select * from phone01 where id = 0055827859;
  ```

  ![image-20220618221008093](md_img/geomesa%E6%9C%9F%E6%9C%AB%E5%A4%A7%E4%BD%9C%E4%B8%9A2.img/image-20220618221008093.png)

##### 2.3spark查询

读取数据

```
val dfraw = spark.read.csv("/data/ZQ/geomesa/data/phone.csv/phone.csv")
```

![image-20220619170549007](md_img/geomesa%E6%9C%9F%E6%9C%AB%E5%A4%A7%E4%BD%9C%E4%B8%9A2.img/image-20220619170549007.png)

- 时空查询

  ```
  dfraw.filter(expr("_c3 like '2022-06-06T22:58:48'") && expr("_c1 like '113.895246'") && expr("_c2 like '22.775922'")).show()
  ```

  ![image-20220619172044939](md_img/geomesa%E6%9C%9F%E6%9C%AB%E5%A4%A7%E4%BD%9C%E4%B8%9A2.img/image-20220619172044939.png)

- 时间查询

  ```
  dfraw.filter(expr("_c3 like '2022-06-06T22:58:48'")).show()
  ```

  ![image-20220619172330371](md_img/geomesa%E6%9C%9F%E6%9C%AB%E5%A4%A7%E4%BD%9C%E4%B8%9A2.img/image-20220619172330371.png)

- 空间查询

  ```
  dfraw.filter(expr("_c1 like '113.895246'") && expr("_c2 like '22.775922'")).show()
  ```

  ![image-20220619172436124](md_img/geomesa%E6%9C%9F%E6%9C%AB%E5%A4%A7%E4%BD%9C%E4%B8%9A2.img/image-20220619172436124.png)

#### 3.geomesa-hbase导入优化

ingest相关参数

| Argument                 | Description                                                  |
| ------------------------ | ------------------------------------------------------------ |
| `-c, --catalog *`        | The catalog table containing schema metadata                 |
| `-f, --feature-name`     | The name of the schema                                       |
| `-s, --spec`             | The `SimpleFeatureType` specification to create              |
| `-C, --converter`        | The GeoMesa converter used to create `SimpleFeature`s        |
| `--converter-error-mode` | Override the error mode defined by the converter             |
| `-t, --threads`          | Number of parallel threads used                              |
| `--input-format`         | Format of input files (csv, tsv, avro, shp, json, etc)       |
| ``--index`               | Specify a particular GeoMesa index to write to, instead of all indices |
| `--no-tracking`          | This application closes when ingest job is submitted. Useful for launching jobs with a script |
| `--run-mode`             | Must be one of `local` or `distributed` (for map/reduce ingest) |
| `--combine-inputs`       | Combine multiple input files into a single input split (distributed jobs only) |
| `--split-max-size`       | Maximum size of a split in bytes (distributed jobs)          |
| `--src-list`             | Input files are text files with lists of files, one per line, to ingest |
| `--force`                | Suppress any confirmation prompts                            |
| `<files>...`             | Input files to ingest                                        |

##### 3.1使用多个线程导入数据

- 3线程

  ```
  geomesa-hbase ingest --catalog qimo \
  	--feature-name phone03 \
  	--input-format csv \
  	-C conf/phonedata.convert \
  	-s conf/phonedata.sft \
  	-t 3 \
  	"data/split/phone_3/*"
  ```

  ![image-20220623162621637](md_img/geomesa%E6%9C%9F%E6%9C%AB%E5%A4%A7%E4%BD%9C%E4%B8%9A2.img/image-20220623162621637.png)

- 7线程

  ```
  geomesa-hbase ingest --catalog qimo \
  	--feature-name phone04 \
  	--input-format csv \
  	-C conf/phonedata.convert \
  	-s conf/phonedata.sft \
  	-t 7 \
  	"data/split/phone_7/*"
  ```

  ![image-20220623163850680](md_img/geomesa%E6%9C%9F%E6%9C%AB%E5%A4%A7%E4%BD%9C%E4%B8%9A2.img/image-20220623163850680.png)

- 5线程

  ```
  geomesa-hbase ingest --catalog qimo \
  	--feature-name phone05 \
  	--input-format csv \
  	-C conf/phonedata.convert \
  	-s conf/phonedata.sft \
  	-t 5 \
  	"data/split/phone_5/*"
  ```

  ![image-20220623170018346](md_img/geomesa%E6%9C%9F%E6%9C%AB%E5%A4%A7%E4%BD%9C%E4%B8%9A2.img/image-20220623170018346.png)

- 4线程

  ```
  geomesa-hbase ingest --catalog qimo \
  	--feature-name phone06_03 \
  	--input-format csv \
  	-C conf/phonedata.convert \
  	-s conf/phonedata.sft \
  	-t 4 \
  	"data/split/phone_4/*"
  ```

  ![image-20220623214529762](md_img/geomesa%E6%9C%9F%E6%9C%AB%E5%A4%A7%E4%BD%9C%E4%B8%9A2.img/image-20220623214529762.png)

- 2线程

  ```
  geomesa-hbase ingest --catalog qimo \
  	--feature-name phone07 \
  	--input-format csv \
  	-C conf/phonedata.convert \
  	-s conf/phonedata.sft \
  	-t 2 \
  	"data/split/phone_2/*"
  ```

  ![image-20220623172331133](md_img/geomesa%E6%9C%9F%E6%9C%AB%E5%A4%A7%E4%BD%9C%E4%B8%9A2.img/image-20220623172331133.png)

（当然受cpu及磁盘速度及物理内存的限制。活动线程太多的话，频繁的上下文切换很耗CPU。）

总结：当线程数为3时执行效率最高，随着线程的增加频繁的上下文切换很耗CPU，此时执行效率会降低。

##### 3.2指定索引（这个做PPT先不涉及）

- z2

  ```
  geomesa-hbase ingest --catalog qimo \
  	--feature-name phone01_index_z2 \
  	--input-format csv \
  	-C conf/phonedata.convert \
  	-s conf/phonedata.sft \
  	--index z2 \
  	"data/phone.csv"
  ```

#### 4.使用geomesa-hbase导出出租车轨迹

##### 4.1编辑特征文件与转换文件

- 特征文件taxidata.sft

  ```
  geomesa = {
    sfts = {
      example = {
        type-name = "example"
        attributes = [
          { name = "taxiid", type = "String", index = true }
          { name = "double_0", type = "Double", index = false }
          { name = "double_1", type = "Double", index = false }
          { name = "bool", type = "string", index = false }
          { name = "time", type = "Date",   index = false }
          { name = "geom", type = "Point",  index = true,srid = 4326,default = true }
     ]
      }
    }
  }
  ```

- 转换文件taxidata.convert 

  ```
  geomesa.converters.example = {
      "fields" : [
          {
              "name" : "taxiid",
              "transform" : "toString($1)"
          },
          {
              "name" : "double_0",
              "transform" : "toDouble($2)"
          },
          {
              "name" : "double_1",
              "transform" : "toDouble($3)"
          },
          {
              "name" : "bool",
              "transform" : "toString($4)"
          },
          {
              "name" : "geom",
              "transform" : "point($double_0,$double_1)"
          },
          {
              "name" : "time",
              "transform" : "isoDateTime($5)"
          },
      ],
      "format" : "CSV",
      "id-field" : "md5(string2bytes($0))",
      "options" : {
          "encoding" : "UTF-8",
          "error-mode" : "skip-bad-records",
          "parse-mode" : "incremental",
          "validators" : [
              "index"
          ]
      },
      "type" : "delimited-text"
  }
  ```

##### 4.2统计数据中的top-k

```
bin/geomesa-hbase stats-top-k -c qimo -f taxi02 --no-cache -k 2
```

![image-20220623151524571](md_img/geomesa%E6%9C%9F%E6%9C%AB%E5%A4%A7%E4%BD%9C%E4%B8%9A2.img/image-20220623151524571.png)

##### 4.3时间列的处理与序号1中相同，此处直接使用已经导入geomesa-hbase中的数据

- id为22223的出租车轨迹，直接使用leaflet参数导出出租车数据到html文件

  ```
  bin/geomesa-hbase export \
      --output-format leaflet \
      --feature-name taxi02 \
      -q "taxiid = '22223'" \
      --zookeepers 10.103.105.78:2181 \
      --catalog qimo > taxi.html
  ```

  命令执行完后会生成名为taxi.html的html文件，在网页打开此html文件即可看到22223出租车的轨迹点

  <img src="md_img/geomesa%E6%9C%9F%E6%9C%AB%E5%A4%A7%E4%BD%9C%E4%B8%9A2.img/image-20220623144840601.png" alt="image-20220623144840601" style="zoom: 45%;" />

- id为36950的出租车轨迹

  ```
  bin/geomesa-hbase export \
  	--output-format leaflet \
  	--feature-name taxi02 \
  	-q "taxiid = '36950'" \
  	--zookeepers 10.103.105.78:2181 \
  	--catalog qimo > taxi36950.html
  ```

  <img src="md_img/geomesa%E6%9C%9F%E6%9C%AB%E5%A4%A7%E4%BD%9C%E4%B8%9A2.img/image-20220623145146378.png" alt="image-20220623145146378" style="zoom: 80%;" />

- 轨迹点对多的出租车28249轨迹

  ```
  bin/geomesa-hbase export            \
      --output-format leaflet         \
      --feature-name taxi02 \
      -q "taxiid = '28249'" \
      --zookeepers 10.103.105.78:2181       \
      --catalog qimo > taximax.html
  ```

  <img src="md_img/geomesa%E6%9C%9F%E6%9C%AB%E5%A4%A7%E4%BD%9C%E4%B8%9A2.img/image-20220623145228728.png" alt="image-20220623145228728" style="zoom:50%;" />

#### 5.KNN查询

使用geospark.jar作为依赖项进入sparkshell

```
spark-shell --jars /home/hadoop/compress/GeoSparkModified-master/classes/artifacts/geospark_jar/geospark.jar
```

##### 5.1KNN查询

121960条数据

```
import org.datasyslab.geospark.spatialOperator.KNNQuery
import org.datasyslab.geospark.spatialRDD.PointRDD;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Coordinate;

val fact=new GeometryFactory();
val queryPoint=fact.createPoint(new Coordinate(-109.73, 35.08)); //查询点
val objectRDD = new PointRDD(sc, "file:///home/hadoop/compress/GeoSparkModified-master/src/test/resources/arealm.csv", 0, "csv");
val resultSize = KNNQuery.SpatialKnnQuery(objectRDD, queryPoint, 5); //查询邻近查询点的5个点
```

```
import org.datasyslab.geospark.spatialOperator.KNNQuery
import org.datasyslab.geospark.spatialRDD.PointRDD;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Coordinate;

val fact=new GeometryFactory();
val queryPoint=fact.createPoint(new Coordinate(114.030981,22.610269)); //查询点(深圳北站)
val objectRDD = new PointRDD(sc, "file:///home/hadoop/geomesa-hbase_2.11-3.4.0/data/distinct_station_output.csv", 2, "csv");
val resultSize = KNNQuery.SpatialKnnQuery(objectRDD, queryPoint, 3); //查询邻近查询点的5个点


val fact=new GeometryFactory();
val queryPoint=fact.createPoint(new Coordinate(114.030981,22.610269)); //查询点(深圳北站)
val objectRDD = new PointRDD(sc, "file:///home/hadoop/geomesa-hbase_2.11-3.4.0/data/distinct_station_output.csv", 2, "csv");
objectRDD.buildIndex("rtree");
val resultSize = KNNQuery.SpatialKnnQueryUsingIndex(objectRDD, queryPoint, 6);
```

![image-20220623212536334](md_img/geomesa%E6%9C%9F%E6%9C%AB%E5%A4%A7%E4%BD%9C%E4%B8%9A2.img/image-20220623212536334.png)
