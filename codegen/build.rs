/*
 * Copyright 2025 BergLoom
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = std::path::Path::new("../core/proto").canonicalize()?;
    let proto_file = proto_dir.join("iceberg.proto");

    tonic_build::configure()
        .build_server(true)
        .protoc_arg("--experimental_allow_proto3_optional")
        .out_dir("src")
        .compile(&[proto_file], &[proto_dir])
        .expect("Failed to compile protos");

    Ok(())
}
