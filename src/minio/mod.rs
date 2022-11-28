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

mod bucket;
pub mod s3_client;

pub fn minio_router(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("/hello").route(web::get().to(hello)))
        .service(web::resource("/buckets").route(web::post().to(bucket::create_bucket)).route(web::get().to(bucket::list_bucket)))
        .service(web::resource("/buckets/{bucket}").route(web::delete().to(bucket::delete_bucket)))
        .service(web::resource("/buckets/{bucket}/objects").route(web::get().to(bucket::list_object)))
        .service(web::resource("/buckets/{bucket}/objects/{object}").route(web::get().to(bucket::get_object)))
    ;
}

async fn hello() -> HttpResponse {
    HttpResponse::Ok().body("Hello, Minio")
}
