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

use std::path;

use clap::Parser;

use minio_dashboard::minio::s3_client;
use minio_dashboard::util;

#[derive(Parser, Debug)]
#[command()]
struct Args {
    /// Path to the export location
    #[arg(short, long)]
    path: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    util::init();
    let args = Args::parse();
    log::info!("Exporting data to {}", args.path);
    let s3client = s3_client::new();
    let buckets = s3client.list_buckets().await?;
    for bucket in buckets {
        let path = format!("{}{}{}", args.path, path::MAIN_SEPARATOR, bucket.bucket_name);
        log::info!("Exporting bucket {} to {}", bucket.bucket_name, path);
        let objects = s3client.list_objects(bucket.bucket_name.to_string()).await?;
        for object in objects {
            let path = format!("{}{}{}", path, path::MAIN_SEPARATOR, object.object_name.clone());
            std::fs::create_dir_all(path::Path::new(&path).parent().unwrap())?;
            log::info!("Exporting object {} to {}", object.object_name.clone(), path.clone());
            let data = s3client.get_object(bucket.bucket_name.clone(), object.object_name.clone()).await?;
            match std::fs::write(path.clone(), data) {
                Ok(_) => log::info!("Exported object {} to {}", object.object_name.clone(), path),
                Err(e) => log::error!("Error exporting object {} to {}: {}", object.object_name.clone(), path.clone(), e),
            }
        }
    }
    Ok(())
}
