import os
import time

import requests

import nacos
import nacos_config

from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
from gevent import pywsgi

import common_log
from flask import Flask, request, send_file

from prediction_code import model_call, model_training

log = common_log.get_log('server.log', 'debug')
nass_ai_server = ''
# 创建一个服务，赋值给APP
app = Flask(__name__)


# 指定接口访问的路径（set_response是API名称），支持什么请求方式get，post
@app.route('/predict', methods=['post'])
def set_response():
    global data_path
    global nass_ai_server
    try:
        # 获取训练数据的索引值
        data_index = request.form.get('dataIndex')
        model_path = request.form.get('modelPath')
        data_save_path = request.form.get("dataSavePath")
        result_path = request.form.get('resultPath')
        model_name = request.form.get("modelName")
        projectId = request.form.get("projectId")
        num_future_points = request.form.get('numFuturePoints')
        if num_future_points is None:
            num_future_points = 1
        # 如果未传入结果路径，则默认为当前路径
        if data_save_path is None:
            log.info("未传入结果路径，使用默认路径")
            data_save_path = './dic/data/'
        if result_path is None:
            log.info("未传入结果路径，使用默认路径")
            result_path = './dic/results/'
        # 保存二进制文件
        if data_index is None:
            return {'code': 500, 'msg': 'data_index is empty'}
        # 保存文件
        # 设置保存路径
        f = request.files['file']
        if f is not None and f != '':
            # f 不为空的处理逻辑
            data_path = os.path.join(data_save_path, f.filename)
            f.save(data_path)
            log.info("文件保存成功！路径：%s" % data_path)
        else:
            # f 为空的处理逻辑
            log.info("未选择文件进行上传")
        # 异常处理
        data = pd.read_csv(data_path, encoding='utf-8')
        # 划分训练集和测试集
        # 选择模型训练的周
        feature = data['cgi']
        # feature不能为空
        if feature.empty:
            return {'code': 500, 'msg': 'feature is empty'}
        # 表示使用iloc方法进行基于索引的切片操作。其中，:表示选择所有行，1: 7
        # 表示选择从索引1到索引6的列（不包括索引7）,因为索引是从0开始的，所以实际上选择的是第2到第6列,不包含第7列。
        # train_data_x = data.iloc[:, 1:7]

        # "6个连续月流量数据"
        # train_data_x = pd.concat([feature, train_data_x], axis=1)
        # train_data_y = data.iloc[:, 7] # "选择第八列,第7个月流量数据"
        # train_data_y = pd.concat([feature, train_data_y], axis=1)
        # v1_x = np.array(train_data_x.iloc[:, 1:]) # 选择所有行，从第2列开始
        # v1_y = np.array(train_data_y.iloc[:, 1:]) # 选择所有行，从第2列开始
        # 如果未传入模型路径，则进行模型训练
        if model_path is None:
            log.info("模型训练")
            model_path = './dic/models/'
            # 表示使用iloc方法进行基于索引的切片操作。其中，:表示选择所有行，1: 7
            # 表示选择从索引1到索引6的列（不包括索引7）,因为索引是从0开始的，所以实际上选择的是第2到第6列,不包含第7列。
            # 1 到 索引值 ，不包含索引值，格式化字符串
            train_data_x = data.iloc[:, 1:int(data_index)]
            # "6个连续月流量数据"
            train_data_x = pd.concat([feature, train_data_x], axis=1)
            train_data_y = data.iloc[:, int(data_index)]  # "选择第八列,第7个月流量数据"
            train_data_y = pd.concat([feature, train_data_y], axis=1)
            v1_x = np.array(train_data_x.iloc[:, 1:])  # 选择所有行，从第2列开始
            v1_y = np.array(train_data_y.iloc[:, 1:])  # 选择所有行，从第2列开始
            # 调用模型训练方法，异步执行
            executor = ThreadPoolExecutor(max_workers=1)
            # 改为时间戳,防止文件重名，格式化为2019-01-21-13:49:00 形式
            save_path = model_path + model_name
            executor.submit(model_training, v1_x, v1_y, save_path,nass_ai_server,projectId)
            return {'code': 200, 'msg': 'success',
                    'data': '模型训练中，请稍后查看结果!模型前缀为：' + model_name,
                    'extend': model_name}
        # 如果传入模型路径，则进行模型调用
        else:
            log.info("模型调用")
            log.info('模型路径：%s' % model_path)
            log.info('结果路径：%s' % result_path)
            log.info('特征：%s' % feature)
            log.info('数据：%s' % data)
            log.info('数据类型：%s' % type(data))
            prediction_data_x = data.iloc[:, 1:int(data_index)]
            prediction_data_x = pd.concat([feature, prediction_data_x], axis=1)
            v2_x = np.array(prediction_data_x.iloc[:, 1:])
            file_path = model_call(model_path, result_path, feature, v2_x, num_future_points)
            return send_file(file_path, as_attachment=True)
    except Exception as e:
        log.error(e)
        return {'code': 500, 'msg': str(e)}


# 定义返回模型列表接口，返回模型列表，目录是./dic/models/
@app.route('/model_list', methods=['get'])
def model_list():
    model_path = request.form.get('modelPath')
    if model_path is None:
        log.info("未传入模型路径，使用默认路径")
        model_path = './dic/models/'
    return {'code': 200, 'msg': 'success', 'data': os.listdir(model_path)}


# 定义返回结果列表接口，返回结果列表，目录是./dic/results/
@app.route('/result_list', methods=['get'])
def result_list():
    result_path = request.form.get('resultPath')
    if result_path is None:
        log.info("未传入结果路径，使用默认路径")
        result_path = './dic/results/'
    return {'code': 200, 'msg': 'success', 'data': os.listdir(result_path)}


@app.route('/uploader', methods=['GET', 'POST'])
def uploader():
    if request.method == 'POST':
        f = request.files['file']
        print(request.files)
        folder_path = "./dic/uploadFiles/"
        os.makedirs(folder_path, exist_ok=True)
        f.save(os.path.join(folder_path, f.filename))
        return {'code': 200, 'msg': 'success', 'data': '上传成功！'}


# nacos服务
def service_register():
    global nass_ai_server
    # 创建初始nacos连接对象
    nacos_server = nacos.Nacos(host=nacos_config.nacos_ip, username=nacos_config.username,
                               password=nacos_config.password)
    # 配置服务注册的参数
    if nacos_config.server_ip is None or nacos_config.server_ip == '0.0.0.0' or nacos_config.server_ip == '' or nacos_config.server_ip == 'localhost' or nacos_config.server_ip == '127.0.0.1':
        nacos_config.server_ip = get_local_ip()
    if nacos_config.server_port is None or nacos_config.server_port == 0:
        nacos_config.server_port = nacos_config.port
    nacos_server.register_service(service_ip=nacos_config.server_ip, service_port=nacos_config.server_port,
                                  service_name=nacos_config.service_name)
    nacos_server.config(app_config='cmcc', env='dev', file_type='yml', config_name='dipper-simu3d-prod.yml')
    nass_ai_server =  nacos_server.get_server_instance(service_name='dipper-gateway')
    nass_ai_server = nass_ai_server + "/naas-bs/ai/noAuth"

    # 开启监听配置的线程和服务注册心跳进程的健康检查进程
    nacos_server.healthy_check()


import socket


def get_local_ip():
    try:
        # 创建一个 UDP 套接字
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # 连接到外部服务器，这里使用百度的 DNS 服务器
        sock.connect(('114.114.114.114', 80))
        # 获取本地 IP 地址
        ip = sock.getsockname()[0]
        return ip
    except socket.error:
        return '127.0.0.1'


if __name__ == '__main__':
    # 开发环境下，debug=True，生产环境下，debug=False
    # ip = '127.0.0.1'
    # port = 19996
    # app.run(ip, port, debug=True)
    service_register()
    # 获取本机 IP 地址
    ip_address = get_local_ip()
    print('本机IP地址为：' + ip_address)
    print('服务启动成功！')
    # 如果nacos_config.ip 为空,或者为0.0.0.0，则使用本机ip
    if nacos_config.ip is None or nacos_config.ip == '0.0.0.0' or nacos_config.ip == '' or nacos_config.ip == 'localhost' or nacos_config.ip == '127.0.0.1':
        nacos_config.ip = ip_address
    # 获取当前网关的ip地址
    socket.gethostbyname(socket.gethostname())
    # 启动服务
    server = pywsgi.WSGIServer((get_local_ip(), nacos_config.port), app)
    server.serve_forever()
