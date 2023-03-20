# bigquery-snapshot


## 创建IAM service account key
目前BigQuery Snapshot功能仅支持API调用，不支持命令行与sdk。在调用API时，需要显示指定token，该token需要通过IAM service account创建。
```
gcloud iam service-accounts keys create KEY_FILE \
    --iam-account=SA_NAME@PROJECT_ID.iam.gserviceaccount.com
```
