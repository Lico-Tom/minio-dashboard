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

use actix_web::{HttpResponse, web};
use serde::Deserialize;

use crate::minio::s3_client;
use crate::minio::s3_client::CreateBucketReq;

pub async fn create_bucket(req: web::Json<CreateBucketReq>) -> HttpResponse {
    match s3_client::new().create_bucket(req.into_inner()).await {
        Ok(_) => HttpResponse::Ok().body(""),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string())
    }
}

#[derive(Deserialize)]
pub struct BucketPath {
    pub bucket: String,
}

pub async fn delete_bucket(path: web::Path<BucketPath>) -> HttpResponse {
    match s3_client::new().delete_bucket(path.into_inner().bucket.to_owned()).await {
        Ok(_) => HttpResponse::Ok().body(""),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string())
    }
}

pub async fn list_bucket() -> HttpResponse {
    match s3_client::new().list_buckets().await {
        Ok(resp) => {
            HttpResponse::Ok().json(resp)
        }
        Err(err) => {
            HttpResponse::InternalServerError().body(err.to_string())
        }
    }
}

pub async fn list_object(path: web::Path<BucketPath>) -> HttpResponse {
    match s3_client::new().list_objects(path.into_inner().bucket.to_owned()).await {
        Ok(body) =>HttpResponse::Ok().json(body),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string())
    }
}

#[derive(Deserialize)]
pub struct ObjectPath {
    pub object: String,
}

pub async fn get_object(bucket_path: web::Path<BucketPath>, object_path: web::Path<ObjectPath>) -> HttpResponse {
    match s3_client::new().get_object(bucket_path.into_inner().bucket.to_owned(),
                                      object_path.into_inner().object.to_owned()).await {
        Ok(body) => {
            HttpResponse::Ok().body(body)
        }
        Err(err) => {
            HttpResponse::InternalServerError().body(err.to_string())
        }
    }
}
