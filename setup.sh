#!/usr/bin/env bash

# Java 8 설치
sudo apt-get update
sudo apt-get install openjdk-8-jdk

# Python 가상 환경 활성화
source /home/codespace/venv/bin/activate

# JAVA_HOME 환경변수 설정
echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> ~/.bashrc
source ~/.bashrc