# encoding:utf-8

import plugins
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
    version="0.1",
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
        if not self.sql.checkDBTB():
            logger.info(f"{self.tag} Check db tb fail. Exit")
            return
        self.disableThis = False
        self.handlers[Event.ON_HANDLE_CONTEXT] = self.on_handle_context
        logger.info(f"{self.tag} Inited")

    def on_handle_context(self, e_context: EventContext):
        if e_context["context"].type not in [ContextType.TEXT]:
            return
        content = e_context["context"].content
        reply = Reply()
        reply.type = ReplyType.TEXT
        if (len(self.includePrefix)==0 and self.emptyForInclude) or (len(self.includePrefix)>0 and content.startswith(self.includePrefix)):
            contentStriped = content.lstrip(self.includePrefix).lstrip()
            if self.handleGlobalEnable(content, e_context):
                return
            elif self.disableThis:
                return
            logger.debug(f"{self.tag} on_handle_context. contentStriped: %s" % contentStriped)
            if contentStriped == self.cmd['keyList']:
                keys = self.sql.query_keys()
                if keys is not None:
                    keywords = []
                    for info in keys:
                        keywords.append(f"* {info[0]}")
                    if len(keywords) == 0:
                        keywords.append('空列表')
                    else:
                        keywords.append(f"\n没有想要的？\n发送'{self.includePrefix}我想要xxx'提交需求.")
                    reply.content = f"# 已知的关键词列表:\n{'\n'.join(keywords)}"
                    e_context["reply"] = reply
                    e_context.action = EventAction.BREAK_PASS  # 事件结束，并跳过处理context的默认逻辑
                    return
            elif contentStriped == self.cmd['admin']:
                keys = self.sql.query_keys()
                reply.content = f">>>全部指令列表<<<\n{''.join([f'* {cmd} \n' for cmd in self.cmd.values()])}"
                e_context["reply"] = reply
                e_context.action = EventAction.BREAK_PASS  # 事件结束，并跳过处理context的默认逻辑
                return
            elif contentStriped == self.cmd['requireList']:
                requirements = self.sql.query_requirements()
                reqs = []
                if requirements is not None:
                    for req in requirements:
                        reqs.append(f"* {req[0]}")
                if len(reqs) == 0:
                    reqs.append('没有新需求')
                reply.content = f"# 全部需求列表:\n{'\n'.join(reqs)}"
                e_context["reply"] = reply
                e_context.action = EventAction.BREAK_PASS  # 事件结束，并跳过处理context的默认逻辑
                return
            elif contentStriped.startswith(self.cmd['require']):
                contents = contentStriped.split(self.cmd['require'])
                if len(contents)>=2:
                    key = contents[1].strip()
                    if self.sql.require_data(key):
                        reply.content = f"反馈'{key}'成功，请静待更新"
                    else:
                        reply.content = "反馈失败."
                else:
                    reply.content = "参数不足."
                e_context["reply"] = reply
                e_context.action = EventAction.BREAK_PASS  # 事件结束，并跳过处理context的默认逻辑
                return
            elif contentStriped.startswith(self.cmd['add']):
                contents = contentStriped.split(' ')
                if len(contents)>=3:
                    key = contents[1]
                    response = ' '.join(contents[2:])
                    if self.sql.insert_data(key, response):
                        reply.content = f"关键词回复添加成功. \n你可以发送'{self.includePrefix}{key}'获取回复信息"
                    else:
                        reply.content = "关键词添加失败."
                else:
                    reply.content = "参数不足."
                e_context["reply"] = reply
                e_context.action = EventAction.BREAK_PASS  # 事件结束，并跳过处理context的默认逻辑
                return
            elif contentStriped.startswith(self.cmd['remove']):
                contents = contentStriped.split(' ')
                if len(contents)==2:
                    key = contents[1]
                    if key.endswith("*"):
                        result = self.sql.remove_data_all(key.rstrip('*'))
                        if result>=1:
                            reply.content = f"关键词'{key}'的{result}条回复已移除"
                        else:
                            reply.content = "关键词移除失败."
                    else:
                        result = self.sql.remove_key(key)
                        if result == 1:
                            reply.content = f"关键词'{key}'回复已移除"
                        elif result > 1:
                            reply.content = '\n'.join([
                                    f"移除失败，有{result}个相同的关键词.",
                                    f"使用'{self.cmd['remove']} {key}*'删除{result}条回复",
                                    f"使用'{self.cmd['remove']} {key} 回复值'删除指定回复"
                                ])
                        else:
                            reply.content = "关键词移除失败."
                elif len(contents)>=3:
                    key = contents[1]
                    response = ''.join(contents[2:])
                    result = self.sql.remove_key_value(key, response)
                    if result>0:
                        reply.content = f"关键词'{key}'回复'{response}'已移除."
                    else:
                        reply.content = "关键词移除失败."
                else:
                    reply.content = "参数不足."
                e_context["reply"] = reply
                e_context.action = EventAction.BREAK_PASS  # 事件结束，并跳过处理context的默认逻辑
                return
            elif contentStriped.startswith(self.cmd['update']):
                contents = contentStriped.split(' ')
                if len(contents)>=3:
                    key = contents[1]
                    response = ' '.join(contents[2:])
                    res = self.sql.update_data(key, response)
                    if res:
                        reply.content = f"关键词'{key}'回复已更新"
                    else:
                        reply.content = f"关键词更新失败.{res}"
                else:
                    reply.content = "参数不足."
                e_context["reply"] = reply
                e_context.action = EventAction.BREAK_PASS  # 事件结束，并跳过处理context的默认逻辑
                return
            elif contentStriped.startswith(self.cmd['enable']):
                contents = contentStriped.split(' ')
                if len(contents)==2:
                    key = contents[1]
                    if self.sql.set_key_state(key, state="active"):
                        reply.content = f"关键词'{key}'回复已启用."
                    else:
                        reply.content = "关键词状态更新失败."
                elif len(contents)>=3:
                    key = contents[1]
                    response = ' '.join(contents[2:])
                    if self.sql.set_key_state(key, response, "active"):
                        reply.content = f"关键词'{key}'回复已启用."
                    else:
                        reply.content = "关键词状态更新失败."
                else:
                    reply.content = "参数不足."
                e_context["reply"] = reply
                e_context.action = EventAction.BREAK_PASS  # 事件结束，并跳过处理context的默认逻辑
                return
            elif contentStriped.startswith(self.cmd['disable']):
                contents = contentStriped.split(' ')
                if len(contents)==2:
                    key = contents[1]
                    if self.sql.set_key_state(key, state="deactive"):
                        reply.content = f"关键词'{key}'回复已禁用."
                    else:
                        reply.content = "关键词状态禁用失败."
                elif len(contents)>=3:
                    key = contents[1]
                    response = ' '.join(contents[2:])
                    if self.sql.set_key_state(key, response, "deactive"):
                        reply.content = f"关键词'{key}'回复{response}已禁用."
                    else:
                        reply.content = "关键词状态禁用失败."
                else:
                    reply.content = "参数不足."
                e_context["reply"] = reply
                e_context.action = EventAction.BREAK_PASS  # 事件结束，并跳过处理context的默认逻辑
                return
            else:
                res = self.sql.search_data(contentStriped)
                logger.debug(f"getytrer {res}")
                if res:
                    if len(res)>1:
                        result = '\n'.join([f"* 回复{idx}->{resp[0]}:\n\t{resp[1]}" for idx,resp in enumerate(res)])
                    else:
                        result = res[0][1]
                    reply.content = result
                    e_context["reply"] = reply
                    e_context.action = EventAction.BREAK_PASS  # 事件结束，并跳过处理context的默认逻辑
                    return
                elif (len(self.excludePrefix)==0 and not self.emptyForExclude) or (len(self.excludePrefix)>0 and not contentStriped.startswith(self.excludePrefix)):
                    reply.content = f"关键词查询为空\n\n你可以发送'{self.includePrefix}我想要{contentStriped}'进行反馈，然后关注后续公众号推文."
                    e_context["reply"] = reply
                    e_context.action = EventAction.BREAK_PASS  # 事件结束，并跳过处理context的默认逻辑
                    return
        elif (len(self.excludePrefix)==0 and not self.emptyForExclude) or (len(self.excludePrefix)>0 and not content.startswith(self.excludePrefix)):
            reply.content = "帮助信息：\n" + self.helpText
            e_context["reply"] = reply
            e_context.action = EventAction.BREAK_PASS  # 事件结束，并跳过处理context的默认逻辑
            return
        e_context["context"].content = content.lstrip(self.excludePrefix).lstrip()
        e_context.action = EventAction.CONTINUE
        
    def handleGlobalEnable(self, content, e_context)->bool:
        reply = Reply()
        reply.type = ReplyType.TEXT
        if content == self.cmd['enableReply']:
            self.disableThis = False
            reply.content = "回复功能已启用.\n\n" + self.helpText
            e_context["reply"] = reply
            e_context.action = EventAction.BREAK_PASS
            return True
        elif content == self.cmd['disableReply']:
            self.disableThis = True
            reply.content = "回复功能已禁用."
            e_context["reply"] = reply
            e_context.action = EventAction.BREAK_PASS
            return True
        return False

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
            self.cmd = {
                "keyList": configCmd.get('keyList', "关键词列表") if configCmd else "关键词列表", # includePrefix cmd
                "require": "我想要", # includePrefix cmd key
                "admin": configCmd.get('admin', "$管理命令") if configCmd else "$管理命令", # includePrefix cmd
                "requireList": "$需求列表", # includePrefix cmd
                "add": "$add ", # includePrefix cmd key value
                "update": "$update ", # includePrefix cmd key newValue
                "remove": "$remove ", # includePrefix cmd key
                "disable": "$disable ", # includePrefix cmd key
                "enable": "$enable ", # includePrefix cmd key
                "disableReply": configCmd.get('disableReply', "$禁用全部回复") if configCmd else "$禁用全部回复",
                "enableReply": configCmd.get('enableReply', "$启用全部回复") if configCmd else "$启用全部回复"
            }
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
