// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use aws_sdk_s3::{Credentials, Region};
use aws_sdk_s3::types::ByteStream;
use serde::Deserialize;
use serde::Serialize;

use crate::constant;

pub struct S3Client {
    client: aws_sdk_s3::Client,
}

pub fn new() -> S3Client {
    let client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Config::builder()
            .credentials_provider(Credentials::new(
                constant::MINIO_ACCESS_KEY.to_string(),
                constant::MINIO_SECRET_KEY.to_string(),
                None,
                None,
                "faked",
            ))
            .region(Region::new("us-east-1"))
            .endpoint_resolver(aws_sdk_s3::Endpoint::immutable(format!("http://{}:{}", constant::MINIO_HOST.to_string(), constant::MINIO_PORT.to_string()).parse().unwrap()))
            .build(),
    );
    S3Client {
        client,
    }
}

#[derive(Deserialize)]
pub struct CreateBucketReq {
    pub bucket_name: String,
}

#[derive(Serialize, Debug)]
pub struct ListBucketResp {
    pub bucket_name: String,
}

#[derive(Serialize, Debug)]
pub struct ListObjectResp {
    pub object_name: String,
}

impl S3Client {
    pub async fn create_bucket(&self, req: CreateBucketReq) -> Result<(), Box<dyn std::error::Error>> {
        self.client.create_bucket().bucket(req.bucket_name.as_str()).send().await?;
        Ok(())
    }

    pub async fn delete_bucket(&self, bucket_name: String) -> Result<(), Box<dyn std::error::Error>> {
        self.client.delete_bucket().bucket(bucket_name).send().await?;
        Ok(())
    }

    pub async fn list_buckets(&self) -> Result<Vec<ListBucketResp>, Box<dyn std::error::Error>> {
        let resp = self.client.list_buckets().send().await?;
        let buckets = resp.buckets.unwrap();
        let mut bucket_names = Vec::new();
        for bucket in buckets {
            bucket_names.push(ListBucketResp {
                bucket_name: bucket.name.unwrap(),
            });
        }
        Ok(bucket_names)
    }

    pub async fn list_objects(&self, bucket_name: String) -> Result<Vec<ListObjectResp>, Box<dyn std::error::Error>> {
        let resp = self.client.list_objects().bucket(bucket_name).send().await?;
        match resp.contents() {
            Some(contents) => {
                let mut object_names = Vec::new();
                for content in contents {
                    object_names.push(ListObjectResp {
                        object_name: content.key().unwrap().to_string(),
                    });
                }
                Ok(object_names)
            }
            None => Ok(Vec::new()),
        }
    }

    pub async fn put_object(&self, bucket_name: String, object_name: String, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        self.client.put_object().bucket(bucket_name).key(object_name).body(ByteStream::from(data)).send().await?;
        Ok(())
    }

    pub async fn get_object_hex(&self, bucket_name: String, object_name: String) -> Result<String, Box<dyn std::error::Error>> {
        match self.get_object(bucket_name, object_name).await {
            Ok(data) => {
                let hex = hex::encode(data);
                Ok(hex)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn get_object(&self, bucket_name: String, object_name: String) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let resp = self.client.get_object().bucket(bucket_name).key(object_name).send().await?;
        let result = resp.body.collect().await;
        match result {
            Ok(data) => Ok(data.into_bytes().to_vec()),
            Err(e) => Err(Box::new(e)),
        }
    }

    pub async fn delete_object(&self, bucket_name: String, object_name: String) -> Result<(), Box<dyn std::error::Error>> {
        self.client.delete_object().bucket(bucket_name).key(object_name).send().await?;
        Ok(())
    }

    pub async fn upload_file(&self, bucket_name: String, object_name: String, file_path: String) -> Result<(), Box<dyn std::error::Error>> {
        self.client.put_object().bucket(bucket_name).key(object_name).body(ByteStream::from_path(file_path).await.unwrap()).send().await?;
        Ok(())
    }

}
