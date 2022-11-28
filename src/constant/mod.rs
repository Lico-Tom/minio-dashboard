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

use lazy_static::lazy_static;

lazy_static! {
    pub static ref MINIO_HOST: String = std::env::var("MINIO_HOST").unwrap_or_else(|_| "localhost".to_string());
    pub static ref MINIO_PORT: u16 = std::env::var("MINIO_PORT").unwrap_or_else(|_| "9000".to_string()).parse().unwrap();
    pub static ref MINIO_ACCESS_KEY: String = std::env::var("MINIO_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    pub static ref MINIO_SECRET_KEY: String = std::env::var("MINIO_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string());
}
