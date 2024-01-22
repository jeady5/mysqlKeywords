# encoding:utf-8

import plugins
import re
from bridge.context import ContextType
from bridge.reply import Reply, ReplyType
from channel.chat_message import ChatMessage
from common.log import logger
from plugins import *
from .sqlCursor import SqlCursor

@plugins.register(
    name="mysqlKeywords",
    desire_priority=950,
    desc="This plugn allow you to produce reply by querying mysql.",
    version="0.2",
    author="jeady",
)
class MysqlKeywords(Plugin):
    def __init__(self):
        super().__init__()
        self.tag = "[MysqlKeywords]"
        
        if not self.initConfig():
            logger.error(f"{self.tag} load config error. {self.config} Exit")
            return
        self.sql = SqlCursor(
            # self.config['mysql']['host'],
            # self.config['mysql']['user'],
            # self.config['mysql']['passwd'],
            # self.config['mysql']['dbName'],
            # self.config['mysql']['tbName'],
            # self.config['mysql']['port'],
            config=self.config['mysql']
        )
        self.disableThis = not self.sql.checkDBTB()
        if self.disableThis:
            logger.error(f"{self.tag} Check db tb fail. Invalid config.")
        self.handlers[Event.ON_HANDLE_CONTEXT] = self.on_handle_context
        logger.info(f"{self.tag} Inited")

    def on_handle_context(self, e_context: EventContext):
        if e_context["context"].type not in [ContextType.TEXT]:
            return
        content = e_context["context"].content
        contentStriped:str = content.lstrip(self.includePrefix).lstrip()
        logger.debug(f"{self.tag} on_handle_context. contentStriped: %s" % contentStriped)
        if (len(self.includePrefix)==0 and self.emptyForInclude) or (len(self.includePrefix)>0 and content.startswith(self.includePrefix)):
            if contentStriped.startswith("$"):
                if self.handleAdminCmd(contentStriped, e_context):
                    return
            elif self.disableThis and self.handleExclude(contentStriped, e_context, "关键词系统已禁用"):
                return
            if self.handleUserKey(contentStriped, e_context):
                return
        if self.handleExclude(contentStriped, e_context, "帮助信息：\n" + self.helpText):
            return
        e_context["context"].content = contentStriped.lstrip(self.excludePrefix).lstrip()
        e_context.action = EventAction.CONTINUE
    
    """全局说明
    所有handle*开头的方法, 返回值bool说明:
        True:  handle方法已经进行了消息回复;
        False: handle方法未进行处理, 交给下一个handle继续处理.
    """
    def handleUserKey(self, content, e_context):
        logger.debug(f'{self.tag} handleUserKey {content}')
        if self.handleUserRequire(content, e_context):
            return True
        if self.handleKeyList(content, e_context):
            return True
        if self.handleUserQuery(content, e_context):
            return True
        return False

    def handleAdminCmd(self, content, e_context):
        logger.debug(f'{self.tag} handleAdminCmd {content}')
        if self.handleGlobalEnable(content, e_context):
            return True
        if self.handleMysqlConfig(content, e_context):
            return True
        if self.handleAdminCmdReply(content, e_context):
            return True
        if self.handleKeyState(content, e_context):
            return True
        if self.handleAddCmd(content, e_context):
            return True
        if self.handleRemoveCmd(content, e_context):
            return True
        if self.handleUpdateCmd(content, e_context):
            return True
        if self.handleRequireList(content, e_context):
            return True
        return False
    
    def handleKeyList(self, content, e_context):
        logger.debug(f'{self.tag} handleKeyList {content}')
        if content == self.cmd['keyList']:
            keys = self.sql.query_keys()
            if keys is not None:
                keywords = []
                for info in keys:
                    keywords.append(f"{info[1]}".rjust(4) + f"> {info[0]}")
                if len(keywords) == 0:
                    keywords.append('空列表')
                else:
                    keywords.append(f"\n没有想要的？\n发送'{self.includePrefix}我想要xxx'提交需求.")
                reply = f">>>已知的关键词列表<<<\n{'\n'.join(keywords)}"
                e_context["reply"] = self.initReply(reply)
                e_context.action = EventAction.BREAK_PASS  # 事件结束，并跳过处理context的默认逻辑
                return True
        return False

    def handleUserRequire(self, content, e_context):
        logger.debug(f'{self.tag} handleUserRequire {content}')
        if content.startswith(self.cmd['require']):
            contents = content.split(self.cmd['require'])
            if len(contents)>=2:
                key = contents[1].strip()
                if self.sql.require_data(key):
                    reply = f"反馈'{key}'成功，请静待更新"
                else:
                    reply = "反馈失败."
            else:
                reply = "参数不足."
            e_context["reply"] = self.initReply(reply)
            e_context.action = EventAction.BREAK_PASS  # 事件结束，并跳过处理context的默认逻辑
            return True
        else:
            return False
    
    def handleUserQuery(self, content, e_context):
        logger.debug(f'{self.tag} handleUserQuery {content}')
        res = self.sql.search_data(content)
        if res:
            if len(res)>1:
                result = '\n'.join([f"* {idx+1}->{resp[0]}:\n  \t{resp[1]}" for idx,resp in enumerate(res)])
                reply = self.initReply(result)
            else:
                reply = self.initReply(res[0][1], res[0][0])
            e_context["reply"] = reply
            e_context.action = EventAction.BREAK_PASS  # 事件结束，并跳过处理context的默认逻辑
            return True
        else:
            return self.handleExclude(content, e_context, f"关键词查询为空\n\n你可以发送'{self.includePrefix}我想要{content}'进行反馈，然后关注后续公众号推文.")

    def handleRequireList(self, content, e_context):
        logger.debug(f'{self.tag} handleRequireList {content}')
        if content == self.cmd['requireList']:
            requirements = self.sql.query_requirements()
            reqs = []
            if requirements is not None:
                for req in requirements:
                    reqs.append(f"* {req[0]}")
            if len(reqs) == 0:
                reqs.append('没有新需求')
            e_context["reply"] = self.initReply(f"# 全部需求列表:\n{'\n'.join(reqs)}")
            e_context.action = EventAction.BREAK_PASS  # 事件结束，并跳过处理context的默认逻辑
            return True
        return False

    def handleKeyState(self, content, e_context):
        logger.debug(f'{self.tag} handleKeyState {content}')
        if content.startswith(self.cmd['enable']):
            contents = content.split(' ')
            if len(contents)==2:
                key = contents[1]
                if self.sql.set_key_state(key, state="active"):
                    reply = f"关键词'{key}'回复已启用."
                else:
                    reply = "关键词状态更新失败."
            elif len(contents)>=3:
                key = contents[1]
                response = ' '.join(contents[2:])
                if self.sql.set_key_state(key, response, "active"):
                    reply = f"关键词'{key}'回复已启用."
                else:
                    reply = "关键词状态更新失败."
            else:
                reply = "参数不足."
            e_context["reply"] = self.initReply(reply)
            e_context.action = EventAction.BREAK_PASS  # 事件结束，并跳过处理context的默认逻辑
            return True
        elif content.startswith(self.cmd['disable']):
            contents = content.split(' ')
            if len(contents)==2:
                key = contents[1]
                if self.sql.set_key_state(key, state="deactive"):
                    reply = f"关键词'{key}'回复已禁用."
                else:
                    reply = "关键词状态禁用失败."
            elif len(contents)>=3:
                key = contents[1]
                response = ' '.join(contents[2:])
                if self.sql.set_key_state(key, response, "deactive"):
                    reply = f"关键词'{key}'回复{response}已禁用."
                else:
                    reply = "关键词状态禁用失败."
            else:
                reply = "参数不足."
            e_context["reply"] = self.initReply(reply)
            e_context.action = EventAction.BREAK_PASS  # 事件结束，并跳过处理context的默认逻辑
            return True
        else:
            return False
    
    def handleAddCmd(self, content, e_context):
        logger.debug(f'{self.tag} handleAddCmd {content}')
        if content.startswith(self.cmd['add']):
            contents = content.split(' ')
            if len(contents)>=3:
                key = contents[1]
                response = ' '.join(contents[2:])
                res = self.sql.insert_data(key, response)
                if res is True:
                    reply = f"关键词回复添加成功. \n你可以发送'{self.includePrefix}{key}'获取回复信息"
                else:
                    reply = f"添加失败. {res}"
            else:
                reply = "参数不足."
            e_context["reply"] = self.initReply(reply)
            e_context.action = EventAction.BREAK_PASS  # 事件结束，并跳过处理context的默认逻辑
            return True
        return False
        
    def handleRemoveCmd(self, content, e_context):
        logger.debug(f'{self.tag} handleRemoveCmd {content}')
        if content.startswith(self.cmd['remove']):
            contents = content.split(' ')
            if len(contents)==2:
                key = contents[1]
                if key.endswith("*"):
                    result = self.sql.remove_data_all(key.rstrip('*'))
                    if result>=1:
                        reply = f"关键词'{key}'的{result}条回复已移除"
                    else:
                        reply = "关键词移除失败."
                else:
                    result = self.sql.remove_key(key)
                    if result == 1:
                        reply = f"关键词'{key}'回复已移除"
                    elif result > 1:
                        reply = '\n'.join([
                                f"移除失败，有{result}个相同的关键词.",
                                f"使用'{self.cmd['remove']} {key}*'删除{result}条回复",
                                f"使用'{self.cmd['remove']} {key} 回复值'删除指定回复"
                            ])
                    else:
                        reply = "关键词移除失败."
            elif len(contents)>=3:
                key = contents[1]
                response = ''.join(contents[2:])
                result = self.sql.remove_key_value(key, response)
                if result>0:
                    reply = f"关键词'{key}'回复'{response}'已移除."
                else:
                    reply = "关键词移除失败."
            else:
                reply = "参数不足."
            e_context["reply"] = self.initReply(reply)
            e_context.action = EventAction.BREAK_PASS  # 事件结束，并跳过处理context的默认逻辑
            return True
        return False
        
    def handleUpdateCmd(self, content, e_context):
        logger.debug(f'{self.tag} handleUpdateCmd {content}')
        if content.startswith(self.cmd['update']):
            contents = content.split(' ')
            if len(contents)>=3:
                key = contents[1]
                response = ' '.join(contents[2:])
                res = self.sql.update_data(key, response)
                if res:
                    reply = f"关键词'{key}'回复已更新"
                else:
                    reply = f"关键词更新失败.{res}"
            else:
                reply = "参数不足."
            e_context["reply"] = self.initReply(reply)
            e_context.action = EventAction.BREAK_PASS  # 事件结束，并跳过处理context的默认逻辑
            return True
        return False

    def handleExclude(self, content: str, e_context, replyText):
        logger.debug(f'{self.tag} handleExclude {content}')
        if (len(self.excludePrefix)==0 and not self.emptyForExclude) or (len(self.excludePrefix)>0 and not content.startswith(self.excludePrefix)):
            e_context["reply"] = self.initReply(replyText)
            e_context.action = EventAction.BREAK_PASS  # 事件结束，并跳过处理context的默认逻辑
            return True
        return False

    def handleMysqlConfig(self, content: str, e_context):
        logger.debug(f'{self.tag} handleMysqlConfig {content}')
        if content.startswith(self.cmd['configMysql']):
            contents = content.split(' ')
            if len(contents) == 2: # config table
                config = dict(zip(["tbName"], contents[1:]))
            elif len(contents) == 3: # config user passwd
                config = dict(zip(["user", "passwd"], contents[1:]))
            elif len(contents) == 4: # config db user passwd
                config = dict(zip(["dbName", "user", "passwd"], contents[1:]))
            newConfig = {**self.config['mysql'], **config}

            self.sql.setConfig(config=newConfig)
            success = self.sql.checkDBTB()
            if success:
                self.config['mysql'] = newConfig
                enableTip = f"\n\n使用命令 `{self.includePrefix} {self.cmd['enableReply']}` 开启关键词回复."
                reply = f"mysql配置已更新。" + (enableTip if self.disableThis else "")
                super().save_config(self.config)
            else:
                self.sql.setConfig(config=self.config['mysql'])
                reply = "新配置无效，未更新."
            e_context["reply"] = self.initReply(reply)
            e_context.action = EventAction.BREAK_PASS
            return True
    
    def handleAdminCmdReply(self, content, e_context):
        logger.debug(f'{self.tag} handleAdminCmdReply {content}')
        if content == self.cmd['admin']:
            e_context["reply"] = self.initReply(f">>>全部指令列表<<<\n{''.join([f'* {cmd} \n' for cmd in self.cmd.values()])}")
            e_context.action = EventAction.BREAK_PASS  # 事件结束，并跳过处理context的默认逻辑
            return True
        return False
        
    def handleGlobalEnable(self, content, e_context)->bool:
        logger.debug(f'{self.tag} handleGlobalEnable {content}')
        if content == self.cmd['enableReply']:
            self.disableThis = False
            e_context["reply"] = self.initReply("回复功能已启用.\n\n" + self.helpText)
            e_context.action = EventAction.BREAK_PASS
            return True
        elif content == self.cmd['disableReply']:
            self.disableThis = True
            e_context["reply"] = self.initReply("回复功能已禁用.")
            e_context.action = EventAction.BREAK_PASS
            return True
        return False

    def initReply(self, content: str, key: str="", type:ReplyType=ReplyType.TEXT):
        if re.match("^http.*\.(?:gif|jpg|jpeg|bmp|png)$", content):
            type = ReplyType.IMAGE_URL
        elif re.match("^.*\.(?:gif|jpg|jpeg|bmp|png)$", content):
            if os.path.isfile(content):
                content = open(content, 'rb')
                type = ReplyType.IMAGE
        elif re.match("^.*\.(?:mp3|wav)$", content):
            type = ReplyType.VOICE
            pass
        if type == ReplyType.TEXT:
            content = ("" if len(key.strip())==0 else f"* {key}:\n\t") + content
        return Reply(type, content)

    def initConfig(self)->bool:
        curDir = os.path.dirname(__file__)
        templatePath = f"{curDir}/config.json.template"
        targetPath = f"{curDir}/config.json"
        cmd = f"cp '{templatePath}' '{targetPath}'"
        if os.path.isfile(templatePath) and not os.path.isfile(targetPath):
            os.system(cmd)
        if os.path.isfile(targetPath):
            try:
                self.config = super().load_config()
            except Exception as e:
                self.config = None
                logger.error(f"{self.tag} load config file syntax err. {e}")
                return False
        else:
            return False

        if self.config:
            configCmd = self.config.get('cmd')
            self.cmd = {**{
                "keyList": "关键词列表", # includePrefix cmd
                "require": "我想要", # includePrefix cmd key
                "requireList": "$需求列表", # includePrefix cmd
                "add": "$add ", # includePrefix cmd key value
                "update": "$update ", # includePrefix cmd key newValue
                "remove": "$remove ", # includePrefix cmd key
                "disable": "$disable ", # includePrefix cmd key
                "enable": "$enable ", # includePrefix cmd key
            }, **configCmd}
            self.includePrefix = self.config.get('includePrefix', "")
            self.excludePrefix = self.config.get('excludePrefix', "")
            self.emptyForInclude = self.config.get('emptyForInclude', True)
            self.emptyForExclude = self.config.get('emptyForExclude', True)
            self.helpText = "输入'{includePrefix} 关键词'获取内容；\n输入'{excludePrefix} 你的问题'获取非关键词信息。\n输入'{includePrefix} {cmdList}'可以查看受支持的全部关键词。".format(includePrefix=self.includePrefix,
                                excludePrefix=self.excludePrefix, cmdList=self.cmd['keyList'])
            return True
        else:
            return False

    def get_help_text(self, **kwargs):
        return f"输入关键词, 即可获取你想要的一切。\n输入'{self.includePrefix} 关键词列表'可以查看全部受支持的关键词。\n"
