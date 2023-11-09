"""
自定义异常
"""


class ForbiddenException(Exception):
    """
    没有权限，可能是token失效
    """


class NotFoundException(Exception):
    """
    无法找到资源
    """


class InternalException(Exception):
    """
    服务器内部错误
    """
