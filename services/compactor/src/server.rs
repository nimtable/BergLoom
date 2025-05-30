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

use crate::rpc::CompactorServiceImpl;
use bergloom_codegen::compactor::compactor_service_server::CompactorServiceServer;
use std::net::SocketAddr;
use tokio::task::JoinHandle;
use tonic::transport::Server;

pub async fn grpc_compactor_serve(
    listen_addr: SocketAddr,
) -> JoinHandle<Result<(), tonic::transport::Error>> {
    let compactor_srv = CompactorServiceImpl {};

    let server = Server::builder()
        .add_service(CompactorServiceServer::new(compactor_srv))
        .serve(listen_addr);

    tokio::spawn(server)
}
