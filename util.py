"""
nacos operate foundation utils.
"""
import socket
import requests
from requests.exceptions import ConnectionError as RequestConnectionError
import logging
import time as dt
import common_log
from requests import Response
from typing import List, Any
from constants import LOG_FORMAT, DATE_FORMAT
from exception import ForbiddenException, NotFoundException, InternalException

# 日志配置
# logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)
# logger = logging.getLogger("flask.app")
#
# if not logger.hasHandlers():
#     logger = logging
logger = common_log.get_log('server.log', 'debug')
# 默认请求超时时间 5秒
DEFAULT_TIMEOUT = 5


class MediaType:
    """
    http请求参数类型
    """
    MULTIPART_FORM_DATA_VALUE = "multipart/form-data"
    APPLICATION_JSON_VALUE = "application/json"
    APPLICATION_PROBLEM_JSON_VALUE = "application/problem+json"
    APPLICATION_FORM_URLENCODED_VALUE = "application/x-www-form-urlencoded"

    APPLICATION_OCTET_STREAM_VALUE = "application/octet-stream"
    APPLICATION_PDF_VALUE = "application/pdf"
    TEXT_HTML_VALUE = "text/html"
    TEXT_PLAIN_VALUE = "text/plain"
    IMAGE_PNG_VALUE = "image/png"
    # 原始response对象
    ORIGIN_RESPONSE = "response"


def do_request(method, url, *args,
               response_type=MediaType.APPLICATION_JSON_VALUE, **kwargs):
    method = str.upper(method or "GET")
    resp: Response
    logger.debug("[nacos] 请求 - [%s] - %s", method, url)
    if "timeout" not in kwargs:
        kwargs["timeout"] = DEFAULT_TIMEOUT
    if method == "GET":
        url = url + "/"
        for item in args:
            url = url + str(item) + "/"
        url = url[:-1]
        if kwargs.__len__() != 0:
            url = url + "?"
            for item in kwargs:
                url = url + str(item) + "=" + str(kwargs[item]) + "&"
            url = url[:-1]
        logger.debug("请求接口 %s", url)
        resp = requests.get(url, *args, **kwargs)
    if method == "POST":
        resp = requests.post(url, *args, **kwargs)
    if method == "PUT":
        return requests.put(url, *args, **kwargs)
    if resp is None:
        return "Request Error"
    elif resp:
        try:
            if MediaType.ORIGIN_RESPONSE == response_type:
                return resp
            if (not response_type or response_type == MediaType.TEXT_PLAIN_VALUE
                    or response_type == MediaType.TEXT_HTML_VALUE):
                return resp.text
            if response_type == MediaType.APPLICATION_OCTET_STREAM_VALUE:
                return resp.content
            if response_type == MediaType.APPLICATION_JSON_VALUE:
                return resp.json()
        finally:
            resp.close()
    else:
        if resp.status_code == 403:
            raise ForbiddenException()
        if resp.status_code == 404:
            raise NotFoundException()
        if resp.status_code == 500:
            raise InternalException()


class HostClient:
    """
    nacos连接客户端，提供基本的操作方法，每个客户端对应一个host地址
    """
    host = ""
    protocol = "http"
    # 失败尝试次数
    trys = 5
    is_valid = True
    # 是否健康，上次请求成功则标记健康
    is_health = True

    def __init__(self, host="127.0.0.1:8848", protocol="http", trys=5):
        self.host = host
        self.trys = trys
        self.protocol = protocol

    def __get_prefix_uri(self):
        return f"{self.protocol}://{self.host}/nacos/v1"

    def __request(self, uri: str, method: str, *args,
                  data=None, response_type=MediaType.APPLICATION_JSON_VALUE,
                  **kwargs):
        """
        执行接口请求

        Args:
          uri: url地址
          method: url请求方法，e.g. get
          data: body参数
          response_type: 返回结果格式，用于对返回结果自动处理，默认json

        Returns:
          根据返回结果类型处理过的接口调用结果，或者请求失败返回None
        """
        url = self.__get_prefix_uri() + uri
        try:
            result = do_request(method=method, url=url,
                                data=data, response_type=response_type, *args, **kwargs)
            self.is_health = True
            return result
        except RequestConnectionError as e:
            self.trys = self.trys - 1
            logger.warning("[Nacos] URL [%s] 请求失败：%s", url, str(e))
            self.is_health = False

    def login(self, username: str, password: str) -> Any:
        """
        登录

        Args:
          username: 用户名
          password: 密码

        Returns:
          token数据，如果登陆成功
          None，如果失败

        Examples:
          client.login("nacos", "nacos")
        """
        data = self.__request("/auth/login", "post", data={
            "username": username,
            "password": password
        })
        return data

    def regist_service(self, access_token="", params=None):
        """"
        注册服务

        Parameters
        ----------
        access_token : str
            token，通过登录获取
        params : dict
            注册服务用到的参数

        Returns
        -------
        注册结果
        """
        return (self.__request("/ns/instance?accessToken=" +
                               access_token, "post", response_type=MediaType.TEXT_PLAIN_VALUE,
                               params=params or {}) or "请求失败")

    def beat(self, access_token="", params=None) -> Response:
        """
        维持心跳

        Args:
            access_token: token，通过登录获取
            params: 用于维持心跳的请求参数

        Returns:
            response对象
        """
        uri = "/ns/instance/beat?accessToken=" + access_token
        if params:
            for item in params:
                uri = uri + f"&{item}={params[item]}"
        resp = self.__request(uri, "put",
                              response_type=MediaType.ORIGIN_RESPONSE)
        return resp


class HostPool:
    """
    host管理器，后续继承接口请求，并对请求错误做标记

    一组ip每次获取下一个，如果网络请求失败会标记次数，次数到了阈值会临时移除，并在一点时间后重新加入
    """
    idx = 0
    hosts: List[HostClient] = []

    # 失败次数，到达次数后会将ip移动到失败数据中，并且标记失败时间
    _fallback_count = 5
    # 失败后将host放回的间隔
    _fallback_init_sec = 600
    # host和上次失败时间映射
    _fallback_time_dict = {}
    # host和失败次数映射
    _fallback_count_dict = {}
    # 失败恢复时间系数
    _fallback_factor_dict = {}

    def __init__(self, host="127.0.0.1:8848"):
        ar = [str.strip(x) for x in host.split(",")]
        hosts = [x if ":" in x else f"{x}:8848" for x in ar]
        self.hosts = [HostClient(host=x, trys=self._fallback_count)
                      for x in hosts]
        self.size = len(self.hosts)

    def borrow(self) -> HostClient:
        """
        获取一个客户端，客户端包含对nacos的各种操作，每个client都是无状态的服务，绑定一个固定的host.
        如果由于连接超时等多次链接失败会将该host的client暂时移除，无法获取，在等待一段时候后恢复可用.

        Returns:
        获取一个连接对象

        Examples:
        HostPool(host="127.0.0.1:8848").borrow()
        """
        now = dt.time()
        hosts = []
        for h in self.hosts:
            if h.is_valid and h.trys <= 0:
                h.is_valid = False
                self._fallback_time_dict[h.host] = now
            elif not h.is_valid:
                fac = dict.get(self._fallback_factor_dict, h.host, 1)
                if (now - dict.get(self._fallback_time_dict, h.host, 0)
                ) > fac * self._fallback_init_sec:
                    h.is_valid = True
                    # 恢复的时候把时间系数加一
                    self._fallback_factor_dict[h.host] = dict.get(
                        self._fallback_factor_dict, h.host, 1) + 1
            else:
                hosts.append(h)

        size = len(hosts)
        if not size:
            raise Exception("[Nacos] 没有可用的服务地址")
        i = min(self.idx, size - 1)
        logger.debug("[Nacos] Borrow Client : Total [%s], Now [%s]",
                     size, self.idx)
        try:
            return hosts[i]
        finally:
            self.idx = (i + 1) % size


def get_host_ip():
    """
    获取本机ip

    Returns:
    本机ip地址
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip


def get_access_token(host: HostPool, username="", password="") -> Any:
    # pylint: disable=line-too-long
    """
    获取nacos认证token

    Args:
      host: 连接池
      username: 用户名
      password: 密码

    Returns:
      如果成功：{"accessToken": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJuYWNvcyIsImV4cCI6MTY0Njg5MzYzMH0.5ZeE0htGExkKrnp99sVKetue02obkx7OsJ85rHFMZIY", "tokenTtl": 18000, "globalAdmin": True, "username": "nacos"}
      如果失败：None
    """
    data = None
    try:
        while True:
            h = host.borrow()
            if not h or not h.is_health:
                return data
            data = h.login(username=username, password=password)
            if data:
                return data
    except RequestConnectionError as e:
        logger.error("连接超时 %s", str(e))
    except Exception as e:
        logger.exception(e, exc_info=True)
    return data
