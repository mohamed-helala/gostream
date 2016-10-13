#!/bin/bash
echo "Downloading Go V1.6.2"
wget https://storage.googleapis.com/golang/go1.6.2.linux-amd64.tar.gz
echo "Installing Go"
tar -xvf go1.6.2.linux-amd64.tar.gz
sudo mv go /usr/local/go
export PATH=$PATH:/usr/local/go/bin
export GOROOT=/usr/local/go
export GOPATH=/vagrant/gostream/
echo "export PATH=$PATH:/usr/local/go/bin" >> ~/.bashrc
echo "export GOROOT=/usr/local/go" >> ~/.bashrc
echo "export GOPATH=/vagrant/gostream/" >> ~/.bashrc
rm -rf go1.6.2.linux-amd64.tar.gz
echo "Installing git"
sudo apt-get -y install git
go get github.com/twmb/algoimpl/go/graph
go get github.com/satori/go.uuid
go get github.com/docker/libchan
go get github.com/docker/spdystream
go get github.com/gorilla/websocket
go get github.com/dmcgowan/msgpack
