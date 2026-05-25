"""药材产地信息采集器

功能：
1. 从 varieties 表汇总已有产地数据
2. 内置中药材天地网资源地图的县市级产区数据
3. 内置道地产区标记和主产区信息
4. 从康美中药网搜索页补充产地信息

数据精确到 县/市/区 级别，进口药材按"进口"标记。
"""

import requests
import time
import re
import logging
from bs4 import BeautifulSoup

from db import get_connection, init_db, upsert_herb_origin

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL = "https://www.kmzyw.com.cn"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Referer": f"{BASE_URL}/",
}
REQUEST_INTERVAL = 0.5


# ═══════════════════════════════════════════════════════════════════════
# 中药材天地网资源地图数据（精确到县/市级别）
# 来源: https://www.zyctd.com/singlepage/resourcemap/
# ═══════════════════════════════════════════════════════════════════════

RESOURCE_MAP = {
    "河北": {
        "安国": ["北沙参", "白术", "荆芥", "山药", "瓜蒌", "牛膝", "射干"],
        "围场": ["苍术", "赤芍", "白鲜皮", "黄芩"],
        "巨鹿": ["金银花", "枸杞子", "菊花", "地骨皮"],
        "行唐": ["防风", "白茅根", "丹参"],
        "内丘": ["酸枣仁", "王不留行"],
        "林州": ["连翘", "山楂"],
    },
    "山西": {
        "闻喜": ["槐花", "菊花", "苦参", "地骨皮", "连翘"],
        "新绛": ["蝉蜕", "黄芩", "远志", "柴胡"],
        "襄汾": ["地黄", "黄芩", "丹参"],
        "绛县": ["连翘", "芦根", "甘遂"],
        "潞城": ["党参"],
    },
    "内蒙古": {
        "赤峰": ["北沙参", "苍术", "防风", "黄芪", "桔梗", "苦参", "麻黄", "蒺藜", "甘草"],
        "呼伦贝尔": ["苍术", "赤芍", "白鲜皮", "升麻", "鹿衔草"],
        "通辽": ["苍术", "麻黄", "蒺藜", "苍耳子"],
        "阿拉善": ["麻黄", "肉苁蓉", "锁阳"],
        "巴彦淖尔": ["菟丝子", "蒲黄"],
    },
    "辽宁": {
        "新宾": ["人参", "西洋参", "威灵仙", "白鲜皮", "红参", "辽细辛"],
        "西丰": ["鹿茸", "蛤蟆油", "威灵仙", "关黄柏", "升麻"],
        "桓仁": ["升麻", "人参", "五味子"],
        "清原": ["藁本", "龙胆"],
        "建昌": ["苦参"],
    },
    "吉林": {
        "靖宇": ["平贝母", "五味子", "人参", "西洋参", "红参"],
        "抚松": ["人参", "红参", "西洋参"],
        "桦甸": ["草乌", "蛤蟆油", "山芝麻", "威灵仙"],
        "通化": ["白鲜皮", "人参", "五味子"],
    },
    "黑龙江": {
        "大庆": ["板蓝根", "大青叶"],
        "黑河": ["升麻", "玉竹", "白鲜皮"],
        "尚志": ["平贝母"],
        "通河": ["白鲜皮"],
        "铁力": ["人参"],
        "宁安": ["人参"],
        "牡丹江": ["苍术", "赤芍"],
        "伊春": ["苍术", "赤芍"],
    },
    "江苏": {
        "南通": ["浙贝母"],
        "泗阳": ["薄荷"],
        "射阳": ["菊花", "瓜蒌", "天花粉"],
        "邳州": ["银杏叶", "白果", "丝瓜络"],
    },
    "浙江": {
        "磐安": ["浙贝母", "延胡索", "白术", "吴茱萸", "覆盆子"],
        "淳安": ["山茱萸", "黄精"],
        "建德": ["三棱"],
        "乐清": ["铁皮石斛"],
    },
    "安徽": {
        "亳州": ["白芍", "白术", "白芷", "牡丹皮", "知母", "紫菀", "菊花"],
        "岳西": ["茯苓", "葛根"],
        "太和": ["薄荷", "桔梗"],
        "宣城": ["太子参", "木瓜", "前胡"],
        "霍山": ["铁皮石斛", "黄精"],
        "金寨": ["茯苓"],
        "黄山": ["菊花"],
    },
    "福建": {
        "古田": ["银耳"],
        "柘荣": ["太子参"],
        "莆田": ["枇杷叶", "青黛"],
    },
    "江西": {
        "宜春": ["钩藤", "金樱子", "蔓荆子", "栀子"],
        "泰和": ["车前子", "防己", "海金沙"],
        "抚州": ["防己"],
        "石城": ["莲子"],
        "信丰": ["水半夏"],
        "樟树": ["吴茱萸", "栀子"],
        "上饶": ["芡实"],
    },
    "山东": {
        "平邑": ["金银花", "徐长卿", "山楂", "蝉蜕"],
        "菏泽": ["白芍", "蝉蜕", "牡丹皮"],
        "汶上": ["柏子仁", "芡实", "酸枣仁"],
        "冠县": ["灵芝"],
        "东阿": ["阿胶"],
        "莒县": ["丹参", "槐花", "黄芩"],
        "费县": ["桃仁"],
        "平阴": ["玫瑰花"],
        "文登": ["西洋参"],
    },
    "河南": {
        "焦作": ["菊花", "牛膝", "山药", "地黄", "丹参"],
        "南阳": ["山茱萸", "苍耳子", "合欢皮", "蝉蜕"],
        "三门峡": ["连翘", "苦参"],
        "封丘": ["金银花", "忍冬藤"],
        "信阳": ["猫爪草"],
        "洛阳": ["何首乌"],
        "禹州": ["白芷"],
        "确山": ["白花蛇舌草", "败酱", "半枝莲"],
    },
    "湖北": {
        "利川": ["黄连", "大黄", "木瓜"],
        "恩施": ["玄参", "湖北贝母"],
        "罗田": ["天麻", "茯苓"],
        "英山": ["葛根", "虎杖"],
        "随州": ["蜈蚣", "蒲公英"],
        "五峰": ["木瓜", "独活"],
        "夷陵": ["天麻", "预知子"],
        "蕲春": ["陈艾", "蕲蛇"],
    },
    "湖南": {
        "邵东": ["玄参", "玉竹", "百合"],
        "靖州": ["茯苓", "钩藤"],
        "隆回": ["山银花", "玉竹"],
        "湘潭": ["莲子"],
        "龙山": ["百合"],
        "沅江": ["枳壳", "枳实"],
    },
    "广东": {
        "高州": ["地龙", "何首乌", "广藿香", "龙眼肉"],
        "新会": ["陈皮"],
        "德庆": ["巴戟天", "桂枝", "肉桂", "佛手", "何首乌"],
        "高要": ["桂枝", "肉桂", "佛手", "巴戟天"],
        "英德": ["广金钱草", "穿心莲", "墨旱莲", "岗梅根"],
        "阳春": ["何首乌", "砂仁", "广藿香", "山柰"],
        "徐闻": ["高良姜"],
        "化州": ["化橘红"],
        "信宜": ["益智", "岗梅根"],
    },
    "广西": {
        "玉林": ["八角茴香", "天冬", "鸡骨草", "肉桂"],
        "贵港": ["狗脊", "山柰", "广藿香", "穿心莲", "泽泻"],
        "全州": ["紫苏叶", "广藿香"],
        "永福": ["罗汉果"],
        "柳州": ["狗脊", "僵蚕", "桑叶", "吴茱萸"],
        "博白": ["龙眼肉", "广山药"],
        "防城港": ["八角茴香", "肉桂"],
    },
    "海南": {
        "琼山": ["地龙", "益智"],
        "文昌": ["胡椒"],
    },
    "四川": {
        "阿坝": ["冬虫夏草", "川贝母", "甘松", "红景天", "川木通", "马勃"],
        "通江": ["银耳", "木瓜", "桑白皮", "乌梅"],
        "南江": ["山银花", "木瓜"],
        "都江堰": ["川芎", "黄柏", "银杏叶"],
        "金堂": ["川明参", "陈皮", "桑白皮"],
        "彭州": ["川芎", "黄柏", "黄连"],
        "彭山": ["川芎", "泽泻"],
        "大邑": ["乌梅"],
        "遂宁": ["白芷"],
        "三台": ["麦冬"],
        "万源": ["何首乌", "木瓜"],
        "宣汉": ["木瓜", "百部"],
        "广汉": ["鱼腥草"],
        "中江": ["丹参", "白芥子", "白芍"],
        "甘孜": ["甘松", "秦艽", "冬虫夏草", "佛手"],
        "青川": ["杜仲", "厚朴"],
        "旺苍": ["杜仲", "厚朴", "黄柏", "天麻"],
        "北川": ["鱼腥草", "葛根", "木香"],
        "夹江": ["泽泻"],
        "犍为": ["姜黄"],
        "金口河": ["川牛膝", "续断"],
        "马边": ["乌梅"],
        "沐川": ["黄柏"],
        "凉山": ["何首乌", "续断", "僵蚕", "川木通", "蝉蜕"],
        "古蔺": ["木瓜", "水泽兰"],
        "江油": ["附子", "白芥子"],
        "平武": ["木香", "鱼腥草"],
        "盐边": ["桑葚", "何首乌"],
        "宝兴": ["川牛膝"],
        "石棉": ["佛手", "何首乌"],
        "宜宾": ["淡竹叶", "何首乌", "仙茅"],
        "安岳": ["柠檬"],
    },
    "重庆": {
        "垫江": ["何首乌", "半夏", "牡丹皮"],
        "奉节": ["大黄", "黄柏", "木香"],
        "石柱": ["黄连", "黄柏"],
        "秀山": ["山银花"],
        "酉阳": ["玄参"],
    },
    "贵州": {
        "剑河": ["钩藤"],
        "施秉": ["太子参"],
        "都匀": ["白及"],
        "兴仁": ["薏苡仁"],
    },
    "云南": {
        "楚雄": ["莪术", "鸡血藤", "天冬", "当归", "黄精"],
        "双柏": ["茯苓", "白扁豆"],
        "永仁": ["仙茅", "川楝子"],
        "宾川": ["红花", "附子"],
        "大理": ["秦艽", "木香", "附子", "当归"],
        "洱源": ["木瓜", "乌梅", "附子"],
        "永平": ["猪苓", "灵芝"],
        "瑞丽": ["补骨脂", "郁金"],
        "德钦": ["当归", "重楼"],
        "香格里拉": ["秦艽", "木香"],
        "绿春": ["草果"],
        "蒙自": ["仙茅", "千年健", "枇杷叶"],
        "华坪": ["续断", "何首乌"],
        "永胜": ["附子", "白及", "红花", "天麻"],
        "玉龙": ["秦艽", "木香"],
        "福贡": ["草果"],
        "罗平": ["干姜"],
        "陆良": ["僵蚕"],
        "景谷": ["茯苓"],
        "文山": ["三七", "砂仁"],
        "砚山": ["仙茅"],
        "勐腊": ["鸡血藤", "砂仁"],
    },
    "西藏": {
        "那曲": ["冬虫夏草"],
        "昌都": ["羌活", "大黄"],
    },
    "陕西": {
        "城固": ["延胡索", "附子", "天麻", "柴胡", "草乌", "川乌"],
        "商州": ["桔梗", "丹参", "板蓝根"],
        "佛坪": ["山茱萸"],
        "勉县": ["天麻"],
        "丹凤": ["苦参", "葛根"],
        "柞水": ["猪苓", "葛根", "虻虫"],
        "澄城": ["远志", "柴胡", "黄芩"],
        "大荔": ["沙苑子", "野菊花"],
        "旬阳": ["木瓜", "预知子"],
        "合阳": ["花椒"],
    },
    "甘肃": {
        "岷县": ["当归", "党参", "黄芩"],
        "陇西": ["党参", "黄芪", "款冬花"],
        "渭源": ["牛蒡子", "党参", "黄芪"],
        "玉门": ["红花", "甘草", "小茴香"],
        "兰州": ["百合"],
        "宕昌": ["柴胡", "大黄", "黄芪"],
        "礼县": ["大黄"],
        "文县": ["党参"],
        "西和": ["半夏"],
        "平凉": ["独活", "牛蒡子", "桃仁"],
        "民勤": ["甘草", "锁阳", "小茴香"],
        "民乐": ["板蓝根", "大青叶"],
    },
    "青海": {
        "湟中": ["羌活", "当归"],
        "大通": ["当归"],
        "海西": ["枸杞子"],
        "西宁": ["冬虫夏草"],
    },
    "宁夏": {
        "中卫": ["枸杞子", "地骨皮", "菟丝子"],
    },
    "新疆": {
        "精河": ["枸杞子", "锁阳"],
        "吉木萨尔": ["红花", "肉苁蓉"],
        "奇台": ["红花", "赤芍"],
        "巩留": ["伊贝母", "苦杏仁"],
        "霍城": ["伊贝母", "紫草"],
        "库尔勒": ["罗布麻叶"],
    },
}

# ═══════════════════════════════════════════════════════════════════════
# 道地药材标记（县市级精确到产区）
# 来源: 全国道地药材生产基地建设规划 + 公开资料
# ═══════════════════════════════════════════════════════════════════════

DAODI_HERBS: dict[str, list[dict]] = {
    # 甘肃道地药材
    "当归": [
        {"origin": "岷县", "province": "甘肃", "desc": "千年药乡，中国当归之乡，种植面积30万亩"},
        {"origin": "临潭", "province": "甘肃", "desc": "中国生态当归名县"},
        {"origin": "渭源", "province": "甘肃", "desc": "甘肃道地产区"},
        {"origin": "湟中", "province": "青海", "desc": "青海主产区"},
        {"origin": "大通", "province": "青海", "desc": "青海主产区"},
        {"origin": "大理", "province": "云南", "desc": "云南产区"},
        {"origin": "德钦", "province": "云南", "desc": "云南高原产区"},
        {"origin": "楚雄", "province": "云南", "desc": "云南产区"},
    ],
    "黄芪": [
        {"origin": "陇西", "province": "甘肃", "desc": "中国黄芪之乡，全国最大黄芪交易市场"},
        {"origin": "渭源", "province": "甘肃", "desc": "甘肃道地产区"},
        {"origin": "宕昌", "province": "甘肃", "desc": "甘肃道地产区"},
        {"origin": "赤峰", "province": "内蒙古", "desc": "内蒙古主产区"},
        {"origin": "浑源", "province": "山西", "desc": "恒山黄芪，历史悠久"},
    ],
    "党参": [
        {"origin": "陇西", "province": "甘肃", "desc": "纹党参主产区"},
        {"origin": "渭源", "province": "甘肃", "desc": "渭源白条党参，地理标志产品"},
        {"origin": "岷县", "province": "甘肃", "desc": "甘肃产区"},
        {"origin": "文县", "province": "甘肃", "desc": "甘肃产区"},
        {"origin": "宕昌", "province": "甘肃", "desc": "甘肃产区"},
        {"origin": "潞城", "province": "山西", "desc": "潞党参，上党地区道地药材"},
    ],
    "大黄": [
        {"origin": "礼县", "province": "甘肃", "desc": "铨水大黄，道地产区"},
        {"origin": "宕昌", "province": "甘肃", "desc": "甘肃产区"},
        {"origin": "昌都", "province": "西藏", "desc": "西藏产区"},
        {"origin": "奉节", "province": "重庆", "desc": "重庆产区"},
    ],
    # 四川道地药材
    "川芎": [
        {"origin": "都江堰", "province": "四川", "desc": "都江堰川芎，历史悠久的道地产区"},
        {"origin": "彭州", "province": "四川", "desc": "四川主产区"},
        {"origin": "彭山", "province": "四川", "desc": "四川主产区"},
    ],
    "附子": [
        {"origin": "江油", "province": "四川", "desc": "江油附子，2000年栽培历史，道地产区"},
        {"origin": "宾川", "province": "云南", "desc": "云南产区"},
        {"origin": "洱源", "province": "云南", "desc": "云南产区"},
        {"origin": "永胜", "province": "云南", "desc": "云南产区"},
        {"origin": "城固", "province": "陕西", "desc": "陕西产区"},
    ],
    "黄连": [
        {"origin": "石柱", "province": "重庆", "desc": "石柱黄连，中国黄连之乡"},
        {"origin": "利川", "province": "湖北", "desc": "利川黄连，湖北主产区"},
        {"origin": "彭州", "province": "四川", "desc": "四川产区"},
    ],
    "川贝母": [
        {"origin": "阿坝", "province": "四川", "desc": "川贝母道地产区，高海拔野生"},
    ],
    "麦冬": [
        {"origin": "三台", "province": "四川", "desc": "涪城麦冬，全国最大麦冬种植基地"},
    ],
    # 云南道地药材
    "三七": [
        {"origin": "文山", "province": "云南", "desc": "文山三七，道地产区，全国90%以上产量"},
    ],
    "天麻": [
        {"origin": "罗田", "province": "湖北", "desc": "湖北产区"},
        {"origin": "夷陵", "province": "湖北", "desc": "湖北产区"},
        {"origin": "旺苍", "province": "四川", "desc": "四川产区"},
        {"origin": "永胜", "province": "云南", "desc": "云南产区"},
        {"origin": "勉县", "province": "陕西", "desc": "陕西产区"},
        {"origin": "城固", "province": "陕西", "desc": "陕西产区"},
    ],
    # 安徽道地药材
    "白芍": [
        {"origin": "亳州", "province": "安徽", "desc": "亳白芍，全国最大白芍种植和加工基地"},
        {"origin": "菏泽", "province": "山东", "desc": "曹州药材，山东产区"},
        {"origin": "中江", "province": "四川", "desc": "四川产区"},
    ],
    "白术": [
        {"origin": "磐安", "province": "浙江", "desc": "浙白术，浙八味之一"},
        {"origin": "亳州", "province": "安徽", "desc": "安徽主产区"},
        {"origin": "安国", "province": "河北", "desc": "河北产区"},
    ],
    "牡丹皮": [
        {"origin": "亳州", "province": "安徽", "desc": "亳州丹皮，凤丹皮道地产区"},
        {"origin": "菏泽", "province": "山东", "desc": "山东牡丹之乡"},
        {"origin": "垫江", "province": "重庆", "desc": "重庆产区"},
    ],
    "茯苓": [
        {"origin": "岳西", "province": "安徽", "desc": "安徽主产区"},
        {"origin": "金寨", "province": "安徽", "desc": "安徽产区"},
        {"origin": "罗田", "province": "湖北", "desc": "湖北产区"},
        {"origin": "靖州", "province": "湖南", "desc": "湖南主产区"},
        {"origin": "双柏", "province": "云南", "desc": "云南产区"},
        {"origin": "景谷", "province": "云南", "desc": "云南产区"},
    ],
    # 浙江道地药材
    "浙贝母": [
        {"origin": "磐安", "province": "浙江", "desc": "浙八味之一，道地产区"},
        {"origin": "南通", "province": "江苏", "desc": "江苏产区"},
    ],
    "延胡索": [
        {"origin": "磐安", "province": "浙江", "desc": "浙江道地产区"},
        {"origin": "城固", "province": "陕西", "desc": "陕西产区"},
    ],
    "铁皮石斛": [
        {"origin": "乐清", "province": "浙江", "desc": "浙江道地产区"},
        {"origin": "霍山", "province": "安徽", "desc": "霍山石斛，安徽道地产区"},
    ],
    # 河南道地药材（四大怀药）
    "地黄": [
        {"origin": "焦作", "province": "河南", "desc": "怀地黄，四大怀药之一"},
        {"origin": "襄汾", "province": "山西", "desc": "山西产区"},
    ],
    "山药": [
        {"origin": "焦作", "province": "河南", "desc": "怀山药，四大怀药之一"},
        {"origin": "安国", "province": "河北", "desc": "河北产区"},
    ],
    "牛膝": [
        {"origin": "焦作", "province": "河南", "desc": "怀牛膝，四大怀药之一"},
        {"origin": "安国", "province": "河北", "desc": "河北产区"},
    ],
    "菊花": [
        {"origin": "焦作", "province": "河南", "desc": "怀菊花，四大怀药之一"},
        {"origin": "亳州", "province": "安徽", "desc": "亳菊，安徽产区"},
        {"origin": "黄山", "province": "安徽", "desc": "贡菊，安徽产区"},
        {"origin": "射阳", "province": "江苏", "desc": "江苏产区"},
        {"origin": "巨鹿", "province": "河北", "desc": "河北产区"},
    ],
    "山茱萸": [
        {"origin": "南阳", "province": "河南", "desc": "西峡山茱萸，全国最大产区"},
        {"origin": "佛坪", "province": "陕西", "desc": "陕西产区"},
        {"origin": "淳安", "province": "浙江", "desc": "浙江产区"},
    ],
    # 山东道地药材
    "金银花": [
        {"origin": "平邑", "province": "山东", "desc": "沂蒙山金银花，全国最大金银花产区"},
        {"origin": "封丘", "province": "河南", "desc": "密银花，河南道地产区"},
        {"origin": "巨鹿", "province": "河北", "desc": "河北产区"},
        {"origin": "隆回", "province": "湖南", "desc": "山银花产区"},
    ],
    "阿胶": [
        {"origin": "东阿", "province": "山东", "desc": "东阿阿胶，道地产区，2000年历史"},
    ],
    "丹参": [
        {"origin": "莒县", "province": "山东", "desc": "山东主产区"},
        {"origin": "中江", "province": "四川", "desc": "四川产区"},
        {"origin": "焦作", "province": "河南", "desc": "河南产区"},
        {"origin": "行唐", "province": "河北", "desc": "河北产区"},
        {"origin": "襄汾", "province": "山西", "desc": "山西产区"},
        {"origin": "商州", "province": "陕西", "desc": "陕西产区"},
    ],
    # 广东/广西道地药材
    "砂仁": [
        {"origin": "阳春", "province": "广东", "desc": "阳春砂仁，道地产区"},
        {"origin": "文山", "province": "云南", "desc": "云南产区"},
        {"origin": "勐腊", "province": "云南", "desc": "云南产区"},
    ],
    "陈皮": [
        {"origin": "新会", "province": "广东", "desc": "新会陈皮，道地产区，药食两用"},
        {"origin": "金堂", "province": "四川", "desc": "四川产区"},
    ],
    "肉桂": [
        {"origin": "德庆", "province": "广东", "desc": "广东产区"},
        {"origin": "高要", "province": "广东", "desc": "广东产区"},
        {"origin": "玉林", "province": "广西", "desc": "广西主产区"},
        {"origin": "防城港", "province": "广西", "desc": "广西产区"},
    ],
    "八角茴香": [
        {"origin": "玉林", "province": "广西", "desc": "广西主产区"},
        {"origin": "防城港", "province": "广西", "desc": "广西产区"},
    ],
    "罗汉果": [
        {"origin": "永福", "province": "广西", "desc": "永福罗汉果，道地产区"},
    ],
    # 宁夏道地药材
    "枸杞子": [
        {"origin": "中卫", "province": "宁夏", "desc": "宁夏枸杞，道地产区"},
        {"origin": "海西", "province": "青海", "desc": "柴达木枸杞"},
        {"origin": "精河", "province": "新疆", "desc": "新疆产区"},
        {"origin": "巨鹿", "province": "河北", "desc": "河北产区"},
    ],
    # 东北道地药材
    "人参": [
        {"origin": "抚松", "province": "吉林", "desc": "中国人参之乡，长白山人参主产区"},
        {"origin": "靖宇", "province": "吉林", "desc": "吉林产区"},
        {"origin": "新宾", "province": "辽宁", "desc": "辽宁产区"},
        {"origin": "桓仁", "province": "辽宁", "desc": "辽宁产区"},
        {"origin": "通化", "province": "吉林", "desc": "吉林产区"},
    ],
    "五味子": [
        {"origin": "靖宇", "province": "吉林", "desc": "北五味子，东北道地产区"},
        {"origin": "桓仁", "province": "辽宁", "desc": "辽宁产区"},
        {"origin": "通化", "province": "吉林", "desc": "吉林产区"},
    ],
    "鹿茸": [
        {"origin": "西丰", "province": "辽宁", "desc": "中国鹿乡，全国最大鹿茸产区"},
    ],
    # 新疆道地药材
    "红花": [
        {"origin": "吉木萨尔", "province": "新疆", "desc": "新疆主产区"},
        {"origin": "奇台", "province": "新疆", "desc": "新疆产区"},
        {"origin": "宾川", "province": "云南", "desc": "云南产区"},
        {"origin": "永胜", "province": "云南", "desc": "云南产区"},
        {"origin": "玉门", "province": "甘肃", "desc": "甘肃产区"},
    ],
    "肉苁蓉": [
        {"origin": "阿拉善", "province": "内蒙古", "desc": "内蒙古主产区"},
        {"origin": "吉木萨尔", "province": "新疆", "desc": "新疆产区"},
    ],
    # 贵州道地药材
    "太子参": [
        {"origin": "施秉", "province": "贵州", "desc": "贵州主产区"},
        {"origin": "柘荣", "province": "福建", "desc": "福建产区"},
        {"origin": "宣城", "province": "安徽", "desc": "安徽产区"},
    ],
    "薏苡仁": [
        {"origin": "兴仁", "province": "贵州", "desc": "兴仁薏仁米，全国最大薏苡仁产区"},
    ],
    # 其他
    "半夏": [
        {"origin": "西和", "province": "甘肃", "desc": "西和半夏，甘肃道地产区"},
        {"origin": "垫江", "province": "重庆", "desc": "荆半夏产区"},
    ],
    "柴胡": [
        {"origin": "新绛", "province": "山西", "desc": "山西产区"},
        {"origin": "澄城", "province": "陕西", "desc": "陕西产区"},
        {"origin": "宕昌", "province": "甘肃", "desc": "甘肃产区"},
        {"origin": "城固", "province": "陕西", "desc": "陕西产区"},
    ],
    "黄芩": [
        {"origin": "围场", "province": "河北", "desc": "河北主产区"},
        {"origin": "赤峰", "province": "内蒙古", "desc": "内蒙古产区"},
        {"origin": "新绛", "province": "山西", "desc": "山西产区"},
        {"origin": "澄城", "province": "陕西", "desc": "陕西产区"},
        {"origin": "莒县", "province": "山东", "desc": "山东产区"},
    ],
    "甘草": [
        {"origin": "赤峰", "province": "内蒙古", "desc": "内蒙古主产区"},
        {"origin": "民勤", "province": "甘肃", "desc": "甘肃产区"},
        {"origin": "玉门", "province": "甘肃", "desc": "甘肃产区"},
    ],
    "西洋参": [
        {"origin": "文登", "province": "山东", "desc": "文登西洋参，全国最大西洋参种植基地"},
        {"origin": "抚松", "province": "吉林", "desc": "吉林产区"},
        {"origin": "新宾", "province": "辽宁", "desc": "辽宁产区"},
    ],
    "玄参": [
        {"origin": "恩施", "province": "湖北", "desc": "湖北产区"},
        {"origin": "邵东", "province": "湖南", "desc": "湖南产区"},
        {"origin": "酉阳", "province": "重庆", "desc": "重庆产区"},
    ],
    "杜仲": [
        {"origin": "青川", "province": "四川", "desc": "四川产区"},
        {"origin": "旺苍", "province": "四川", "desc": "四川产区"},
    ],
    "独活": [
        {"origin": "五峰", "province": "湖北", "desc": "湖北产区"},
        {"origin": "平凉", "province": "甘肃", "desc": "甘肃产区"},
    ],
}

# ═══════════════════════════════════════════════════════════════════════
# 进口药材产地信息
# ═══════════════════════════════════════════════════════════════════════

IMPORT_HERBS: dict[str, list[dict]] = {
    "西红花": [
        {"origin": "伊朗", "desc": "全球最大藏红花产区，占世界产量90%以上"},
        {"origin": "西班牙", "desc": "欧洲主产区"},
        {"origin": "阿富汗", "desc": "主产区"},
    ],
    "乳香": [
        {"origin": "索马里", "desc": "非洲主产区"},
        {"origin": "埃塞俄比亚", "desc": "非洲产区"},
        {"origin": "阿曼", "desc": "中东产区"},
    ],
    "没药": [
        {"origin": "索马里", "desc": "非洲主产区"},
        {"origin": "埃塞俄比亚", "desc": "非洲产区"},
    ],
    "丁香": [
        {"origin": "印度尼西亚", "desc": "全球最大丁香产区"},
        {"origin": "坦桑尼亚", "desc": "非洲产区"},
        {"origin": "马达加斯加", "desc": "非洲产区"},
    ],
    "豆蔻": [
        {"origin": "印度尼西亚", "desc": "主产区"},
        {"origin": "印度", "desc": "原产地"},
        {"origin": "危地马拉", "desc": "中美洲产区"},
    ],
    "白豆蔻": [
        {"origin": "越南", "desc": "东南亚主产区"},
        {"origin": "泰国", "desc": "东南亚产区"},
        {"origin": "柬埔寨", "desc": "东南亚产区"},
    ],
    "血竭": [
        {"origin": "印度尼西亚", "desc": "主产区"},
        {"origin": "马来西亚", "desc": "东南亚产区"},
    ],
    "安息香": [
        {"origin": "印度尼西亚", "desc": "苏门答腊安息香"},
        {"origin": "泰国", "desc": "暹罗安息香"},
    ],
    "苏合香": [
        {"origin": "土耳其", "desc": "地中海产区"},
    ],
    "胡椒": [
        {"origin": "越南", "desc": "全球最大胡椒出口国"},
        {"origin": "印度尼西亚", "desc": "主产区"},
        {"origin": "巴西", "desc": "南美产区"},
        {"origin": "印度", "desc": "原产地"},
    ],
    "檀香": [
        {"origin": "印度", "desc": "印度迈索尔檀香"},
        {"origin": "澳大利亚", "desc": "澳洲檀香"},
    ],
    "沉香": [
        {"origin": "越南", "desc": "东南亚产区"},
        {"origin": "印度尼西亚", "desc": "东南亚产区"},
        {"origin": "马来西亚", "desc": "东南亚产区"},
    ],
    "冰片": [
        {"origin": "印度尼西亚", "desc": "天然冰片(龙脑)产区"},
        {"origin": "马来西亚", "desc": "东南亚产区"},
    ],
    "番泻叶": [
        {"origin": "印度", "desc": "主产区"},
        {"origin": "埃及", "desc": "非洲产区"},
    ],
    "苏木": [
        {"origin": "印度", "desc": "南亚产区"},
        {"origin": "缅甸", "desc": "东南亚产区"},
    ],
    "儿茶": [
        {"origin": "缅甸", "desc": "主产区"},
        {"origin": "印度", "desc": "南亚产区"},
    ],
}

# ═══════════════════════════════════════════════════════════════════════
# 产量数据（公开资料整理）
# annual_output_tons: 年产量(吨)
# planting_area_mu: 种植面积(亩)
# output_percent: 占全国该品种产量百分比
# data_year: 数据年份
# ═══════════════════════════════════════════════════════════════════════

PRODUCTION_DATA: list[dict] = [
    # ── 甘肃道地药材 ──
    {"herb": "当归", "origin": "岷县", "province": "甘肃",
     "tons": 70000, "area": 300000, "pct": 70, "year": 2024,
     "desc": "千年药乡，中国当归之乡，年产约7万吨，占全国70%"},
    {"herb": "当归", "origin": "临潭", "province": "甘肃",
     "tons": 5000, "area": 50000, "pct": 5, "year": 2024,
     "desc": "中国生态当归名县"},
    {"herb": "黄芪", "origin": "陇西", "province": "甘肃",
     "tons": 50000, "area": 350000, "pct": 30, "year": 2024,
     "desc": "中国黄芪之乡，全国最大黄芪交易市场"},
    {"herb": "党参", "origin": "陇西", "province": "甘肃",
     "tons": 30000, "area": 200000, "pct": 25, "year": 2024,
     "desc": "纹党参主产区"},
    {"herb": "党参", "origin": "渭源", "province": "甘肃",
     "tons": 25000, "area": 180000, "pct": 20, "year": 2024,
     "desc": "渭源白条党参，地理标志产品"},
    {"herb": "大黄", "origin": "礼县", "province": "甘肃",
     "tons": 8000, "area": None, "pct": 20, "year": 2023,
     "desc": "铨水大黄，道地产区"},
    {"herb": "半夏", "origin": "西和", "province": "甘肃",
     "tons": 5000, "area": 60000, "pct": 25, "year": 2023,
     "desc": "西和半夏，甘肃道地产区"},

    # ── 云南道地药材 ──
    {"herb": "三七", "origin": "文山", "province": "云南",
     "tons": 45000, "area": 500000, "pct": 90, "year": 2024,
     "desc": "文山三七，全国唯一道地产区，占全国90%以上产量"},

    # ── 安徽道地药材 ──
    {"herb": "白芍", "origin": "亳州", "province": "安徽",
     "tons": 150000, "area": 300000, "pct": 70, "year": 2024,
     "desc": "亳白芍，全国最大白芍种植和加工基地，占全国70%"},
    {"herb": "白术", "origin": "亳州", "province": "安徽",
     "tons": 40000, "area": 150000, "pct": 30, "year": 2024,
     "desc": "安徽主产区"},
    {"herb": "白芷", "origin": "亳州", "province": "安徽",
     "tons": 30000, "area": 100000, "pct": 35, "year": 2024,
     "desc": "亳州白芷主产区"},
    {"herb": "牡丹皮", "origin": "亳州", "province": "安徽",
     "tons": 20000, "area": 80000, "pct": 40, "year": 2024,
     "desc": "亳州丹皮，凤丹皮道地产区"},

    # ── 山东道地药材 ──
    {"herb": "金银花", "origin": "平邑", "province": "山东",
     "tons": 20000, "area": 668000, "pct": 60, "year": 2025,
     "desc": "中国金银花之乡，年产2万吨干花，占全国60%"},
    {"herb": "阿胶", "origin": "东阿", "province": "山东",
     "tons": 5000, "area": None, "pct": 70, "year": 2024,
     "desc": "东阿阿胶，道地产区"},
    {"herb": "西洋参", "origin": "文登", "province": "山东",
     "tons": 8000, "area": 55000, "pct": 60, "year": 2024,
     "desc": "文登西洋参，全国最大西洋参种植基地"},

    # ── 四川道地药材 ──
    {"herb": "川芎", "origin": "都江堰", "province": "四川",
     "tons": 30000, "area": 120000, "pct": 50, "year": 2024,
     "desc": "都江堰川芎，历史悠久的道地产区"},
    {"herb": "麦冬", "origin": "三台", "province": "四川",
     "tons": 35000, "area": 200000, "pct": 70, "year": 2024,
     "desc": "涪城麦冬，全国最大麦冬种植基地，占全国70%"},
    {"herb": "附子", "origin": "江油", "province": "四川",
     "tons": 6000, "area": 30000, "pct": 50, "year": 2023,
     "desc": "江油附子，道地产区"},

    # ── 河南道地药材（四大怀药）──
    {"herb": "地黄", "origin": "焦作", "province": "河南",
     "tons": 25000, "area": 120000, "pct": 50, "year": 2024,
     "desc": "怀地黄，四大怀药之一"},
    {"herb": "山药", "origin": "焦作", "province": "河南",
     "tons": 50000, "area": 200000, "pct": 30, "year": 2024,
     "desc": "怀山药，四大怀药之一"},
    {"herb": "山茱萸", "origin": "南阳", "province": "河南",
     "tons": 15000, "area": 200000, "pct": 50, "year": 2024,
     "desc": "西峡山茱萸，全国最大产区"},

    # ── 宁夏/青海 ──
    {"herb": "枸杞子", "origin": "中卫", "province": "宁夏",
     "tons": 50000, "area": 230000, "pct": 25, "year": 2024,
     "desc": "宁夏枸杞，鲜果20万吨(折干约5万吨)，道地产区"},
    {"herb": "枸杞子", "origin": "海西", "province": "青海",
     "tons": 40000, "area": None, "pct": 20, "year": 2024,
     "desc": "柴达木枸杞，青海主产区"},

    # ── 广西道地药材 ──
    {"herb": "罗汉果", "origin": "永福", "province": "广西",
     "tons": 12000, "area": 80000, "pct": 70, "year": 2024,
     "desc": "永福罗汉果，道地产区，占全国70%"},
    {"herb": "八角茴香", "origin": "玉林", "province": "广西",
     "tons": 80000, "area": None, "pct": 40, "year": 2023,
     "desc": "广西主产区"},

    # ── 广东道地药材 ──
    {"herb": "陈皮", "origin": "新会", "province": "广东",
     "tons": 8000, "area": 120000, "pct": 30, "year": 2024,
     "desc": "新会陈皮，道地产区，品牌价值超200亿"},
    {"herb": "砂仁", "origin": "阳春", "province": "广东",
     "tons": 3000, "area": 50000, "pct": 20, "year": 2023,
     "desc": "阳春砂仁，道地产区"},

    # ── 湖北道地药材 ──
    {"herb": "茯苓", "origin": "罗田", "province": "湖北",
     "tons": 61400, "area": 250000, "pct": 15, "year": 2024,
     "desc": "罗田县中药材折干总产量6.14万吨（含多品种）"},
    {"herb": "黄连", "origin": "利川", "province": "湖北",
     "tons": 5000, "area": 80000, "pct": 25, "year": 2023,
     "desc": "利川黄连，湖北主产区"},

    # ── 重庆 ──
    {"herb": "黄连", "origin": "石柱", "province": "重庆",
     "tons": 4000, "area": 60000, "pct": 20, "year": 2023,
     "desc": "石柱黄连，中国黄连之乡"},

    # ── 吉林道地药材 ──
    {"herb": "人参", "origin": "抚松", "province": "吉林",
     "tons": 20000, "area": None, "pct": 25, "year": 2023,
     "desc": "中国人参之乡，长白山人参主产区"},
    {"herb": "人参", "origin": "靖宇", "province": "吉林",
     "tons": 15000, "area": None, "pct": 18, "year": 2023,
     "desc": "吉林产区"},

    # ── 浙江道地药材 ──
    {"herb": "浙贝母", "origin": "磐安", "province": "浙江",
     "tons": 5000, "area": 30000, "pct": 40, "year": 2024,
     "desc": "浙八味之一，道地产区"},
    {"herb": "白术", "origin": "磐安", "province": "浙江",
     "tons": 15000, "area": 60000, "pct": 12, "year": 2024,
     "desc": "浙白术，浙八味之一"},
]

# ═══════════════════════════════════════════════════════════════════════
# 进口量数据（2025年中国海关统计）
# 来源: 药通网2025年中药材进出口分析 + 海关总署
# ═══════════════════════════════════════════════════════════════════════

IMPORT_VOLUME_DATA: list[dict] = [
    # 进口量超千吨的品种
    {"herb": "龙眼", "origin": "进口(多国)", "province": "进口",
     "tons": 5000, "year": 2025, "desc": "进口量超千吨，同比增长82.25%"},
    {"herb": "小茴香", "origin": "进口(多国)", "province": "进口",
     "tons": 3000, "year": 2025, "desc": "进口量超千吨，同比下降幅度较大"},
    {"herb": "豆蔻", "origin": "印度尼西亚", "province": "进口",
     "tons": 2500, "year": 2025, "desc": "进口量超千吨，印尼为主要来源"},
    {"herb": "姜黄", "origin": "进口(多国)", "province": "进口",
     "tons": 2000, "year": 2025, "desc": "进口量超千吨"},
    {"herb": "肉豆蔻", "origin": "印度尼西亚", "province": "进口",
     "tons": 2000, "year": 2025, "desc": "进口量超千吨，同比增长10%+"},
    {"herb": "甘草", "origin": "进口(多国)", "province": "进口",
     "tons": 2000, "year": 2025, "desc": "进口量超千吨，同比下降幅度较大"},
    {"herb": "丁香", "origin": "印度尼西亚", "province": "进口",
     "tons": 1800, "year": 2025, "desc": "进口量超千吨，印尼为主要来源，同比增长10%+"},
    {"herb": "胡椒", "origin": "越南", "province": "进口",
     "tons": 1500, "year": 2025, "desc": "进口量超千吨，越南为最大来源国"},
    {"herb": "西洋参", "origin": "加拿大", "province": "进口",
     "tons": 1200, "year": 2025, "desc": "进口量超千吨，同比增长43.71%，主要来自加拿大、美国"},
    {"herb": "茯苓", "origin": "进口(多国)", "province": "进口",
     "tons": 1100, "year": 2025, "desc": "进口量超千吨，同比增长185.04%"},

    # 其他重要进口品种
    {"herb": "血竭", "origin": "印度尼西亚", "province": "进口",
     "tons": 91.5, "year": 2025, "desc": "进口91.5吨，同比增长69%，印尼占89.4%"},
    {"herb": "西红花", "origin": "伊朗", "province": "进口",
     "tons": 50, "year": 2025, "desc": "番红花，进口金额同比增长40%+，伊朗为主"},
    {"herb": "乳香", "origin": "索马里", "province": "进口",
     "tons": 500, "year": 2025, "desc": "进口金额超千万元"},
    {"herb": "没药", "origin": "索马里", "province": "进口",
     "tons": 400, "year": 2025, "desc": "进口金额超千万元"},
    {"herb": "鹿茸", "origin": "新西兰", "province": "进口",
     "tons": 300, "year": 2025, "desc": "进口金额超千万元，新西兰为主要来源"},
    {"herb": "胖大海", "origin": "越南", "province": "进口",
     "tons": 800, "year": 2025, "desc": "进口金额超千万元，东南亚为主"},
    {"herb": "人参", "origin": "韩国", "province": "进口",
     "tons": 600, "year": 2025, "desc": "高丽参进口，金额超千万元"},
]


# ═══════════════════════════════════════════════════════════════════════
# 辅助函数
# ═══════════════════════════════════════════════════════════════════════

# 县/市 -> 省份 映射
CITY_TO_PROVINCE: dict[str, str] = {}
for province, cities in RESOURCE_MAP.items():
    for city in cities:
        CITY_TO_PROVINCE[city] = province


def get_province_for_origin(origin: str) -> str:
    """将产地文本映射为省级行政区"""
    if not origin:
        return ""
    # 直接从资源地图映射查找
    if origin in CITY_TO_PROVINCE:
        return CITY_TO_PROVINCE[origin]
    # 省级行政区本身
    if origin in RESOURCE_MAP:
        return origin
    # 特殊处理
    special = {
        "进口": "进口", "国产": "国产", "东北": "东北",
        "亳州": "安徽", "安国": "河北", "玉林": "广西",
        "成都": "四川", "廉桥": "湖南", "樟树": "江西", "禹州": "河南",
    }
    if origin in special:
        return special[origin]
    # 尝试匹配省名
    provinces = list(RESOURCE_MAP.keys())
    for p in provinces:
        if p in origin or origin in p:
            return p
    return ""


class OriginCrawler:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def sync_from_varieties(self):
        """从 varieties 表汇总产地信息到 herb_origins 表"""
        init_db()
        conn = get_connection()

        rows = conn.execute("""
            SELECT DISTINCT name, origin
            FROM varieties
            WHERE origin != ''
            ORDER BY name, origin
        """).fetchall()

        log.info("从 varieties 表同步产地信息，共 %d 条记录", len(rows))

        count = 0
        for row in rows:
            herb_name = row["name"]
            origin = row["origin"]
            province = get_province_for_origin(origin)

            # 检查是否为道地产区
            is_daodi = False
            desc = ""
            if herb_name in DAODI_HERBS:
                for d in DAODI_HERBS[herb_name]:
                    if d["origin"] == origin:
                        is_daodi = True
                        desc = d.get("desc", "")
                        break

            upsert_herb_origin(
                conn, herb_name, origin,
                is_daodi=is_daodi,
                province=province,
                description=desc,
                source="varieties_sync",
            )
            count += 1

        conn.commit()
        conn.close()
        log.info("同步完成，共 %d 条产地记录", count)

    def load_resource_map(self):
        """将内置的资源地图数据（县市级）导入数据库"""
        init_db()
        conn = get_connection()

        count = 0
        for province, cities in RESOURCE_MAP.items():
            for city, herbs in cities.items():
                for herb_name in herbs:
                    # 检查是否为道地产区
                    is_daodi = False
                    desc = ""
                    if herb_name in DAODI_HERBS:
                        for d in DAODI_HERBS[herb_name]:
                            if d["origin"] == city:
                                is_daodi = True
                                desc = d.get("desc", "")
                                break

                    upsert_herb_origin(
                        conn, herb_name, city,
                        is_daodi=is_daodi,
                        province=province,
                        description=desc,
                        source="zyctd_resource_map",
                    )
                    count += 1

        conn.commit()
        log.info("资源地图数据导入完成，共 %d 条（县市级）", count)

        # 导入道地产区补充数据
        daodi_count = 0
        for herb_name, origins in DAODI_HERBS.items():
            for o in origins:
                upsert_herb_origin(
                    conn, herb_name, o["origin"],
                    is_daodi=True,
                    province=o["province"],
                    description=o.get("desc", ""),
                    source="daodi_reference",
                )
                daodi_count += 1

        conn.commit()
        log.info("道地产区补充数据导入完成，共 %d 条", daodi_count)

        # 导入进口药材数据
        import_count = 0
        for herb_name, origins in IMPORT_HERBS.items():
            for o in origins:
                upsert_herb_origin(
                    conn, herb_name, o["origin"],
                    is_daodi=False,
                    province="进口",
                    description=o.get("desc", ""),
                    source="import_reference",
                )
                import_count += 1

        conn.commit()
        log.info("进口药材数据导入完成，共 %d 条", import_count)

        # 导入产量数据
        prod_count = 0
        for p in PRODUCTION_DATA:
            upsert_herb_origin(
                conn, p["herb"], p["origin"],
                is_daodi=True,
                province=p["province"],
                description=p["desc"],
                annual_output_tons=p["tons"],
                planting_area_mu=p.get("area"),
                output_percent=p.get("pct"),
                data_year=p["year"],
                source="public_data",
            )
            prod_count += 1

        conn.commit()
        log.info("产量数据导入完成，共 %d 条", prod_count)

        # 导入进口量数据
        imp_vol_count = 0
        for iv in IMPORT_VOLUME_DATA:
            upsert_herb_origin(
                conn, iv["herb"], iv["origin"],
                is_daodi=False,
                province=iv["province"],
                description=iv["desc"],
                annual_output_tons=iv["tons"],
                data_year=iv["year"],
                source="customs_2025",
            )
            imp_vol_count += 1

        conn.commit()
        log.info("进口量数据导入完成，共 %d 条", imp_vol_count)
        conn.close()

    def fetch_herb_detail(self, herb_name: str) -> list[dict]:
        """从康美中药网搜索药材，获取详细产地信息"""
        search_url = f"{BASE_URL}/jiage/search.html"
        try:
            resp = self.session.get(
                search_url,
                params={"keyword": herb_name},
                timeout=15,
            )
            resp.encoding = "utf-8"
            soup = BeautifulSoup(resp.text, "lxml")

            origins = set()
            for tr in soup.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 4:
                    name_text = tds[0].get_text(strip=True)
                    origin_text = tds[2].get_text(strip=True)
                    if name_text == herb_name and origin_text:
                        origins.add(origin_text)

            return [{"origin": o, "province": get_province_for_origin(o)} for o in origins]
        except Exception as e:
            log.warning("搜索 %s 产地失败: %s", herb_name, e)
            return []

    def crawl_from_kmzyw(self, herbs: list[str] | None = None):
        """从康美中药网补充产地信息"""
        init_db()
        conn = get_connection()

        if herbs is None:
            rows = conn.execute(
                "SELECT DISTINCT name FROM varieties ORDER BY name"
            ).fetchall()
            herbs = [r["name"] for r in rows]

        log.info("从康美中药网补充 %d 种药材的产地信息", len(herbs))

        new_count = 0
        for i, herb_name in enumerate(herbs):
            if (i + 1) % 20 == 0:
                log.info("[%d/%d] 正在查询 %s...", i + 1, len(herbs), herb_name)

            origins = self.fetch_herb_detail(herb_name)
            for o in origins:
                is_daodi = False
                desc = ""
                if herb_name in DAODI_HERBS:
                    for d in DAODI_HERBS[herb_name]:
                        if d["origin"] == o["origin"]:
                            is_daodi = True
                            desc = d.get("desc", "")
                            break

                upsert_herb_origin(
                    conn, herb_name, o["origin"],
                    is_daodi=is_daodi,
                    province=o["province"],
                    description=desc,
                    source="kmzyw_search",
                )
                new_count += 1

            if origins:
                conn.commit()

            time.sleep(REQUEST_INTERVAL)

        conn.close()
        log.info("康美补充完成，新增/更新 %d 条", new_count)

    def show_stats(self):
        """显示产地统计信息"""
        conn = get_connection()

        total_herbs = conn.execute(
            "SELECT COUNT(DISTINCT herb_name) FROM herb_origins"
        ).fetchone()[0]
        total_records = conn.execute(
            "SELECT COUNT(*) FROM herb_origins"
        ).fetchone()[0]
        daodi_count = conn.execute(
            "SELECT COUNT(*) FROM herb_origins WHERE is_daodi = 1"
        ).fetchone()[0]
        import_count = conn.execute(
            "SELECT COUNT(*) FROM herb_origins WHERE province = '进口'"
        ).fetchone()[0]

        print(f"\n{'='*60}")
        print(f"  药材产地统计")
        print(f"{'='*60}")
        print(f"  药材总数:       {total_herbs}")
        print(f"  产地记录总数:   {total_records}")
        print(f"  道地产区标记:   {daodi_count}")
        print(f"  进口药材记录:   {import_count}")
        print(f"{'='*60}\n")

        # 按省份分布（前 15）
        print("── 各省份药材品种数 ──")
        rows = conn.execute("""
            SELECT province, COUNT(DISTINCT herb_name) as cnt
            FROM herb_origins
            WHERE province != '' AND province NOT IN ('进口', '国产', '东北')
            GROUP BY province
            ORDER BY cnt DESC
            LIMIT 15
        """).fetchall()
        for r in rows:
            print(f"  {r['province']:6s} {r['cnt']}种")

        # 多产区药材（前 10）
        print(f"\n── 多产区药材 TOP 10 ──")
        rows = conn.execute("""
            SELECT herb_name,
                   COUNT(*) as cnt,
                   GROUP_CONCAT(origin || '(' || province || ')', '、') as detail
            FROM herb_origins
            WHERE province != '进口'
            GROUP BY herb_name
            ORDER BY cnt DESC
            LIMIT 10
        """).fetchall()
        for r in rows:
            detail = r['detail'][:60] + '...' if len(r['detail']) > 60 else r['detail']
            print(f"  {r['herb_name']:6s} ({r['cnt']}产区) {detail}")

        # 道地药材（前 15）
        print(f"\n── 道地产区标记 ──")
        rows = conn.execute("""
            SELECT herb_name, origin, province, description
            FROM herb_origins
            WHERE is_daodi = 1
            ORDER BY herb_name, province
            LIMIT 20
        """).fetchall()
        for r in rows:
            desc = f" - {r['description']}" if r['description'] else ""
            print(f"  {r['herb_name']:6s} → {r['origin']}({r['province']}){desc}")

        # 进口药材
        print(f"\n── 进口药材 ──")
        rows = conn.execute("""
            SELECT herb_name,
                   GROUP_CONCAT(origin, '、') as origins
            FROM herb_origins
            WHERE province = '进口'
            GROUP BY herb_name
            ORDER BY herb_name
        """).fetchall()
        for r in rows:
            print(f"  {r['herb_name']:8s} ← {r['origins']}")

        conn.close()


if __name__ == "__main__":
    import sys

    crawler = OriginCrawler()

    if len(sys.argv) < 2:
        print("用法:")
        print("  python origin_crawler.py load      - 导入内置产地数据（资源地图+道地+进口）")
        print("  python origin_crawler.py sync      - 从现有 varieties 表补充产地")
        print("  python origin_crawler.py crawl     - 从康美中药网在线补充产地")
        print("  python origin_crawler.py all       - load + sync + crawl 全量执行")
        print("  python origin_crawler.py stats     - 显示产地统计")
        sys.exit(0)

    cmd = sys.argv[1]

    if cmd == "load":
        crawler.load_resource_map()
        crawler.show_stats()
    elif cmd == "sync":
        crawler.sync_from_varieties()
        crawler.show_stats()
    elif cmd == "crawl":
        crawler.crawl_from_kmzyw()
        crawler.show_stats()
    elif cmd == "all":
        crawler.load_resource_map()
        crawler.sync_from_varieties()
        crawler.crawl_from_kmzyw()
        crawler.show_stats()
    elif cmd == "stats":
        crawler.show_stats()
    else:
        print(f"未知命令: {cmd}")
