# breadt-football-bet-ml

使用爬虫爬取足彩数据之后进行机器学习和预测


## 使用docker建立本地数据库

```shell
docker run --name lcm-breadt-mysql -e MYSQL_ROOT_PASSWORD=breadt@2019 -p 3306:3306 -d mysql:5.7
```