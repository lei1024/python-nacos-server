import os
import random
import time

import joblib
import numpy as np
import pandas as pd
import requests
import xgboost as xgb
from urllib.parse import quote
from sklearn.model_selection import (train_test_split)


# 定义实验目录
path = 'dic/'
# 定义数据路径
data_path = path + 'data/VAI_20231023_000120231025110813.csv'
# 定义结果路径
result_path = path + 'results/'


# '''准确率计算'''


def get_accuracy(y_true: np.array, y_pred: np.array):
    """
    准确率函数
    :param y_true: 真实标签
    :param y_pred: 预测标签
    :return:
    """
    arr_diff = abs(y_true.flatten() - y_pred) / y_true.flatten()
    accdata = arr_diff[np.where(arr_diff <= 0.3)]
    acc = round((len(accdata) / len(arr_diff)), 6)
    return acc


def xgb_predict_building(X_train: np.array, X_test: np.array, y_train: np.array, y_test: np.array, iteration: int):
    """
    模型训练
    :param X_train:训练集
    :param X_test:测试集
    :param y_train:训练标签
    :param y_test:测试标签
    :return:
    """
    xgb_regressor = xgb.XGBRegressor(learning_rate=0.01,
                                     n_estimators=400,
                                     max_depth=7,
                                     n_jobs=30,
                                     min_child_weight=1)

    xgb_regressor.fit(X_train, y_train)

    # 打印出当前迭代轮次
    print("第{}轮迭代".format(iteration + 1))
    y_pred = xgb_regressor.predict(X_test)
    acc_30 = get_accuracy(y_test, y_pred)
    print("准确率:", acc_30)
    print("预测值:", y_pred)
    return xgb_regressor, str(acc_30)


def model_training(feature: np.array, label: np.array, savePath: str,ai_server: str,project_id:str):
    """
    模型训练
    :param feature: 特征
    :param label: 标签
    :return: 模型路径
    """
    xgb_regressor = []
    acc_30 = []
    # 模型训练
    for i in range(0, 10):
        rs = random.randint(0, 100)
        X_train, X_test, y_train, y_test = train_test_split(
            feature, label, test_size=0.2, random_state=rs)
        results = xgb_predict_building(
            X_train, X_test, y_train, y_test, i)
        xgb_regressor.append(results[0])
        acc_30.append(results[1])
    # 模型保存
    index = np.argmax(acc_30)
    joblib.dump(xgb_regressor[index], savePath +
                '_acc={}.pkl'.format(acc_30[index]))
    file_path = savePath + '_acc={}.pkl'.format(acc_30[index])
    file_name = os.path.basename(file_path)
    requests.get(ai_server + "/updateModel?modelName=" + quote(file_name) + "&projectId=" + project_id)


# 默认预测1个时间点
def model_call(model_path: str, path_result: str, key: pd.Series, feature: np.array, num_future_points=1):
    """
    模型调用
    :param num_future_points: 默认预测1个时间点
    :param path: 模型路径
    :param key: 主键
    :param feature: 特征
    :return: 无输出
    """
    model_path = './dic/models/' + model_path
    model = joblib.load(model_path)
    # 定义存储预测结果的列表
    future_predictions = []

    # 复制初始特征数据
    updated_features = np.copy(feature)

    # 使用循环进行多次预测
    for i in range(int(num_future_points)):
        # 进行单个时间点的预测
        prediction = model.predict(updated_features)

        # 将预测结果添加到列表中
        future_predictions.append(prediction)

        # 更新特征数据：去掉原来的第一列，并在末尾添加预测结果
        updated_features = np.delete(updated_features, 0, axis=1)  # 删除第一列
        updated_features = np.column_stack((updated_features, prediction))  # 添加预测结果作为新的最后一列

    # 创建一个空的DataFrame用于保存所有时间点的预测结果
    all_predictions = pd.DataFrame()
    # 生成时间戳
    file_name_ = time.strftime("%Y%m%d%H%M%S", time.localtime())
    # 将所有预测结果保存为CSV文件
    file_path = path_result + str(file_name_) + '.csv'
    # 将 future_predictions 转换为二维数组
    array_predictions = np.array(future_predictions)
    # 转置二维数组
    transposed_predictions = array_predictions.T

    # 创建 DataFrame
    df = pd.DataFrame()
    # 添加 key 列
    df['cgi'] = key
    # 将转置后的预测结果添加到 DataFrame 中
    df = pd.concat([df, pd.DataFrame(transposed_predictions)], axis=1)

    # 将 DataFrame 写入文件
    df.to_csv(file_path, index=False)
    result = pd.read_csv(file_path, encoding='utf-8')
    return file_path


'''主函数'''
if __name__ == '__main__':
    # 统计运行时间
    start = time.time()
    data = pd.read_csv(data_path, encoding='gbk')
    # 划分训练集和测试集
    # 选择模型训练的周
    feature = data['cgi']
    # 表示使用iloc方法进行基于索引的切片操作。其中，:表示选择所有行，1: 7
    # 表示选择从索引1到索引6的列（不包括索引7）,因为索引是从0开始的，所以实际上选择的是第2到第6列,不包含第7列。
    # train_data_x = data.iloc[:, 1:7]

    # "6个连续月流量数据"
    # train_data_x = pd.concat([feature, train_data_x], axis=1)
    # train_data_y = data.iloc[:, 7] # "选择第八列,第7个月流量数据"
    # train_data_y = pd.concat([feature, train_data_y], axis=1)
    # v1_x = np.array(train_data_x.iloc[:, 1:]) # 选择所有行，从第2列开始
    # v1_y = np.array(train_data_y.iloc[:, 1:]) # 选择所有行，从第2列开始
    prediction_data_x = data.iloc[:, 1:7]
    prediction_data_x = pd.concat([feature, prediction_data_x], axis=1)
    v2_x = np.array(prediction_data_x.iloc[:, 1:])

    # model_path = model_training(v1_x, v1_y)

    model_call('dic/models/xgb_流量_85_acc=0.775496.pkl', path, feature, v2_x)
    end = time.time()
    print("运行时间：", end - start)

# 程序入参 是否使用模型  模型路径  数据路径  结果路径
# python prediction_code.py 1 ./dic/models/xgb_流量_85_acc=0.775496.pkl ./dic/data/ ./dic/results/
# python prediction_code.py 0 model_path data_path result_path
