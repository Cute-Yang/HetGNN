"""
define a simple logger
"""

import logging
from logging.handlers import TimedRotatingFileHandler
import sys
import requests
from requests.adapters import HTTPAdapter
import os

#the max retries for http connect
MAX_RETRIES=3
s = requests.Session()
s.mount('http://', HTTPAdapter(max_retries=MAX_RETRIES))
s.mount('https://', HTTPAdapter(max_retries=MAX_RETRIES))

class LogFactory(object):
    headers = {"Content-Type": "text/plain"}

    def __init__(
        self, 
        log_dir: str = "sb", 
        log_level: int = logging.INFO, 
        log_prefix="xx.log",
        log_format=None,
        scope_name="xx",
        use_webhook=True,
        webhook_url: str = "", 
        mentioned_list=["@all"],
        use_stream=True,
        file_handler_type="rolling",
        timeout=50
    ):
        """
        Args:
            log_dir:the directory to save log,default is logs which is on current directory!
            log_leve:int,can be warn,info,error,fatal....
            webhook_url:a url which push info
            use_stream:bool,whether show info to other stream
            file_handler_type:str,if rolling,set rolling log by day/normal:a generic a+ mode file
            scope_name:the scope name,to prevent that different loggers write the same content
            mentioned_list:the person list which you want to push info,default for everyone
            timeout:the timeout for net request
        """
        self.log_dir = log_dir
        self.log_level = log_level
        self.use_stream=use_stream
        self.file_handler_type=file_handler_type
        self.timeout=timeout
    
        #optional 
        self.use_webhook=use_webhook
        if use_webhook:
            self.webhook_url = webhook_url
            key_index = webhook_url.find("key")
            if key_index == -1:
                print("the webhook url: {} // missing key.\nif you use this,you can not push file!".format(webhook_url))
                self.url_key = ""
            else:
                self.url_key = webhook_url[key_index:]
            self.mentioned_list=mentioned_list
        self.prefix=log_prefix
        self.format=log_format

        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)

        if not isinstance(self.log_level,int):
            try:
                self.log_level=int(self.log_level)
            except:
                raise RuntimeError("log level should be int or can be converted to int ,but your input is {}".format(self.log_level))
        self._set_logger(
            prefix=self.prefix,
            log_format=self.format,
            scope_name=scope_name
        )

    def _set_logger(self, prefix: str, scope_name:str,log_format: str = None):
        """
        Args:
            prefix:the prefix of log file
        """
        log_fp = os.path.join(self.log_dir, prefix)
        if self.file_handler_type=="rolling":
            file_handler = TimedRotatingFileHandler(
                filename=log_fp,
                when="midnight",
                interval=1,
                backupCount=5, #hard code
                encoding="utf-8"
            )
        elif self.file_handler_type=="normal":
            file_handler=logging.FileHandler(
                filename=log_fp,
                mode="a+",
                encoding="utf-8"
            )
        
        if log_format is None:
            log_format = "%(asctime)s [%(levelname)s] %(filename)s: %(message)s"

        formatter = logging.Formatter(log_format)
        file_handler.setLevel(self.log_level)
        file_handler.setFormatter(formatter)
            
        _logger = logging.getLogger(scope_name)
        _logger.setLevel(self.log_level)
        _logger.addHandler(file_handler)

        #add to stream
        if self.use_stream:
            stream_handler=logging.StreamHandler(stream=sys.stdout)
            stream_handler.setLevel(self.log_level)
            stream_handler.setFormatter(formatter)
            _logger.addHandler(stream_handler)
        self.logger = _logger

    def info(self, msg):
        self.logger.info(msg)

    def warning(self, msg):
        self.logger.warning(msg)

    def error(self, msg,exc_info=True):
        self.logger.error(msg,exc_info=exc_info)

    def fatal(self, msg):
        self.logger.fatal(msg)

    def push_text(self, text):

        """
        Args:
            text:str,text content to push
            mentioned_list:list,members you want to @

        Returns:
            dict,http post returns

        """
        if not self.use_webhook:
            self.logger.warning("you set not use webhook....!")
            return
        data = {
            "msgtype": "text",
            "text": {
                "content": text,
                "mentioned_list": self.mentioned_list,
            }
        }
        try:
            res = requests.post(self.webhook_url, headers=self.headers, json=data,timeout=self.timeout)
            res_json=res.json()
        except Exception as e:
            res_json={"error":str(e)}
        return res_json
    
    def push_markdown(self, markdown: str):
        """
        Args:
            markdown:str,the markdown format text
        
        Returns:
            dict,http post returns
        """
        if not self.use_webhook:
            self.logger.warning("you set not use webhook....!")
            return
        if not markdown.endswith("<@all>"):
            markdown += "<@all>"
        data = {
            "msgtype": "markdown",
            "markdown": {
                "content": markdown
            }
        }
        try:
            res = requests.post(self.webhook_url, headers=self.headers, json=data,timeout=self.timeout)
            res_json=res.json()
        except Exception as e:
            res_json={"error":str(e)}
        return res_json

    def push_image(self, img_base64, img_md5):
        """
        Args:
            img_base64:img convert to base64
            img_md5:check the img
        Returns:
            dict,http post returns
        """
        if not self.use_webhook:
            self.logger.warning("you set not use webhook....!")
            return
        data = {
            "msg_type": "image",
            "image": {
                "base64": img_base64,
                "md5": img_md5
            }
        }
        try:
            res = requests.post(self.webhook_url, headers=self.headers, json=data, timeout=self.timeout)
            res_json = res.json()
        except Exception as e:
            res_json = {"error": str(e)}
        return res_json

    def push_file(self, fp: str):
        """
        Args:
            fp:the file path you want to push
        Returns:
            dict,http post returns
        """
        if not self.use_webhook:
            self.logger.warning("you set not use webhook....!")
            return
        post_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/upload_media?key={}&type={}".format(
            self.url_key,
            "file"  # 固定传入 file
        )

        file_size = os.path.getsize(fp)
        file_data = {
            "filename": fp,
            "filelength": file_size
        }

        file_res = requests.post(
            post_url,
            json=file_data
        )
        file_json = file_res.json()
        if "media_id" in file_json:
            media_id = file_json["media_id"]
            push_data = {
                "msgtype": "file",
                "file": {
                    "meida_id": media_id
                }
            }
            res= requests.post(self.webhook_url, headers=self.headers, json=push_data,timeout=self.timeout)
            res_json=res.json()
            return res_json
        else:
            return file_json

    def __str__(self):
        p_tr=hex(id(self))
        return "<object with log and push info at {}>".format(p_tr)


if __name__=="__main__":
    my_logger=LogFactory(
        log_dir=".",
        log_level=logging.INFO,
        webhook_url="https://www.baidu.com"
    )
    print(my_logger)
