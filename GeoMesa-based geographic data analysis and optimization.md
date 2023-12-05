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
<div align=center>
  ![image-20220623140737824](https://github.com/Aleduohm/GeoMesa/assets/84367663/31e04966-f86a-4e70-8861-d16041f5df50)
</div>

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
<div align=center>
  ![image-20220623140817879](https://github.com/Aleduohm/GeoMesa/assets/84367663/ccc8517f-030e-41bf-973b-a94966a0870a)
</div>

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
<div align=center>
![image-20220606202802822](https://github.com/Aleduohm/GeoMesa/assets/84367663/5a13ab60-a8ad-49b6-88e6-ecde91a6599d)
</div>

在hbase中查看
<div align=center>
![image-20220623141024734](https://github.com/Aleduohm/GeoMesa/assets/84367663/ca860224-8c22-4c8e-aa29-26a3382848c6)
</div>

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
<div align=center>
  ![image-20220618215823991](https://github.com/Aleduohm/GeoMesa/assets/84367663/0b182b20-8c5d-4c35-a136-bdd9e38201fe)
</div>
<div align=center>
  ![image-20220625190127929](https://github.com/Aleduohm/GeoMesa/assets/84367663/90605c56-914a-4d02-845e-bbde41836856)
</div>

- 时间查询

  ```
  bin/geomesa-hbase export -c qimo -f phone01 -q "time = '2022-06-06T22:58:48'"
  ```
<div align=center>
  ![image-20220625163335507](https://github.com/Aleduohm/GeoMesa/assets/84367663/cc0d29f9-c819-4df2-9226-106f7b76d36c)
</div>

- 空间查询

  ```
  bin/geomesa-hbase export -c qimo -f phone01 -q "geom = 'POINT (113.895246 22.775922)'"
  ```

  ```
  bin/geomesa-hbase export -c qimo -f phone01 -q "geom = 'POINT (114.011875 22.6715972)'"
  ```
<div align=center>
  ![image-20220618220510290-16559649503891](https://github.com/Aleduohm/GeoMesa/assets/84367663/19521390-5a34-48dc-a5eb-ceb9b473af74)
</div>

##### 2.2hive查询

导入数据

```
create table phone01(id int,time string,lng double,lat double,datetime string) row format delimited fields terminated by ',';

load data inpath '/data/ZQ/geomesa/data/phone.csv/phone.csv' into table qimo.phone01;
```
<div align=center>
![image-20220618215221132](https://github.com/Aleduohm/GeoMesa/assets/84367663/b0403fb6-5a23-453e-8512-e1d00a4d3810)
</div>

- 时空查询

  ```
  select * from phone01 where time = '2022-06-06T22:58:48' and lng = 113.895246 and lat = 22.775922;
  ```
<div align=center>
  ![image-20220618215741467](https://github.com/Aleduohm/GeoMesa/assets/84367663/7c123442-8198-471d-8998-ec46464cea74)
</div>
- 时间查询

  ```
  select * from phone01 where datetime = '2022-06-06T22:58:48';
  ```
<div align=center>
  ![image-20220618220329110](https://github.com/Aleduohm/GeoMesa/assets/84367663/3c066d97-2f33-4ea5-9189-a26bf8f9a7bc)
</div>

- 空间查询

  ```
  select * from phone01 where lng = 113.895246 and lat = 22.775922;
  ```
<div align=center>
  ![image-20220618220622856](https://github.com/Aleduohm/GeoMesa/assets/84367663/44764c6c-07be-46c9-b11e-7735db849e2c)
</div>

- 一般属性

  ```
  select * from phone01 where id = 0055827859;
  ```
<div align=center>
  ![image-20220618221008093](https://github.com/Aleduohm/GeoMesa/assets/84367663/7d061551-1349-41d9-ba23-ee421d42e52c)
</div>

##### 2.3spark查询

读取数据

```
val dfraw = spark.read.csv("/data/ZQ/geomesa/data/phone.csv/phone.csv")
```
<div align=center>
![image-20220619170549007](https://github.com/Aleduohm/GeoMesa/assets/84367663/a3200a04-5d49-4c5d-8f9e-c5abff242da1)
</div>

- 时空查询

  ```
  dfraw.filter(expr("_c3 like '2022-06-06T22:58:48'") && expr("_c1 like '113.895246'") && expr("_c2 like '22.775922'")).show()
  ```
<div align=center>
  ![image-20220619172044939](https://github.com/Aleduohm/GeoMesa/assets/84367663/ad11bb1d-beda-4cf7-a88d-c586466cf7e4)
</div>

- 时间查询

  ```
  dfraw.filter(expr("_c3 like '2022-06-06T22:58:48'")).show()
  ```
<div align=center>
  ![image-20220619172330371](https://github.com/Aleduohm/GeoMesa/assets/84367663/6eddd55e-11c8-479b-a881-b1e56f822058)
</div>
- 空间查询

  ```
  dfraw.filter(expr("_c1 like '113.895246'") && expr("_c2 like '22.775922'")).show()
  ```
<div align=center>
  ![image-20220619172436124](https://github.com/Aleduohm/GeoMesa/assets/84367663/fa708579-0367-4122-a867-61d0dcb64ec4)
</div>

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
<div align=center>
  ![image-20220623162621637](https://github.com/Aleduohm/GeoMesa/assets/84367663/f447cf06-5db1-4a25-9d5c-b2bf0a025e0b)
</div>

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
<div align=center>
  ![image-20220623163850680](https://github.com/Aleduohm/GeoMesa/assets/84367663/54f59a5b-f3ee-4d68-9b1c-5a022f862f1b)
</div>

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
<div align=center>
  ![image-20220623170018346](https://github.com/Aleduohm/GeoMesa/assets/84367663/d094566b-61e0-4339-bd3b-7b8045336c92)
</div>

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
<div align=center>
  ![image-20220623214529762](https://github.com/Aleduohm/GeoMesa/assets/84367663/4c7273fe-d842-4b4d-84c9-1603f361e561)
</div>

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
<div align=center>
  ![image-20220623172331133](https://github.com/Aleduohm/GeoMesa/assets/84367663/45d02a3e-34f5-4f34-905d-e254a6d58b35)
</div>

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
<div align=center>
![image-20220623151524571](https://github.com/Aleduohm/GeoMesa/assets/84367663/e17c70c3-2542-4382-9e68-86179fb0c6f9)
</div>

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

  <div align=center>
    <img src="https://github.com/Aleduohm/GeoMesa/assets/84367663/ad5f9298-71b7-4450-8bff-354522b12729" alt="image-20220623144840601" style="zoom: 33%;" width="720"/>
  </div>

- id为36950的出租车轨迹

  ```
  bin/geomesa-hbase export \
  	--output-format leaflet \
  	--feature-name taxi02 \
  	-q "taxiid = '36950'" \
  	--zookeepers 10.103.105.78:2181 \
  	--catalog qimo > taxi36950.html
  ```

  <div align=center>
    <img src="https://github.com/Aleduohm/GeoMesa/assets/84367663/d0d08cc8-505e-444a-86dd-7bb408208683" style="zoom: 33%;" width="720"/>
  </div>
- 轨迹点对多的出租车28249轨迹

  ```
  bin/geomesa-hbase export            \
      --output-format leaflet         \
      --feature-name taxi02 \
      -q "taxiid = '28249'" \
      --zookeepers 10.103.105.78:2181       \
      --catalog qimo > taximax.html
  ```

  <div align=center>
    <img src="https://github.com/Aleduohm/GeoMesa/assets/84367663/abb72698-a368-43f1-8341-191117ac621f" style="zoom: 33%;" width="720"/>
  </div>
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

![image-20220623212536334](https://github.com/Aleduohm/GeoMesa/assets/84367663/dda26eb8-d81e-419c-878c-790c842ef9b7)

  <div align=center>
    <img src="https://github.com/Aleduohm/GeoMesa/assets/84367663/e5cf776f-6303-4fbb-980c-98e1fb1e8382" style="zoom: 33%;" width="720"/>
  </div>
