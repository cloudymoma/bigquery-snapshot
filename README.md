# bigquery-snapshot


## 创建IAM service account key
目前BigQuery Snapshot功能仅支持API调用，不支持命令行与sdk。在调用API时，需要显示指定token，该token需要通过IAM service account创建。
```
#将命令行中的KEY_FILE替换为实际存储路径，SA_NAME与PROJECT_ID替换为实际的service account与project id
gcloud iam service-accounts keys create KEY_FILE \
    --iam-account=SA_NAME@PROJECT_ID.iam.gserviceaccount.com
```
## 创建Secret 
利用Goole Cloud Secret Manager服务创建Secret存储上一步创建的service account key file。Cloud Function内的代码在创建BigQuery snapshot时需要引用该secret生成token。
```
#将SECRET_ID替换为实际的名称，将PATH替换为service account key的实际存放路径。
gcloud secrets create SECERT_ID \
    --data-file=PATH
```

## 部署名为bq-snapshot的Cloud Function函数
```
gcloud functions deploy bq-snapshot --gen2 \
--runtime=python311 --region=us-central1 --source=. \
--entry-point=bq_snapshot --trigger-http --timeout=3600 --allow-unauthenticated
```
## 创建名为bq-snapshot-job的cloud scheduler任务
按照crontab的格式设置schedule，将cloud_function_uri替换为cloud function对应的uri，将job-config.json里的文件替换为实际的配置文件。
```
gcloud scheduler jobs create http bq-snapshot-job \
    --schedule "10 */1 * * * " \
    --uri "cloud_function_uri" \
    --message-body-from-file="job-config.json" \
    --location="us-central1" \
    --headers Content-Type=application/json
```
job-config.json模版参考如下，dataset与table全部设置为* 时，Cloud Function会默认备份全部的dataset，以及dataset下全部的表。
```
{
  "project_id" : "<project_id>",
  "project" : "<project_name>",
  "secret_id" : "<secret_name>",
  "version_id" : "<secret_version>",
  "dataset" : "*",
  "table" : "*"
}
```
