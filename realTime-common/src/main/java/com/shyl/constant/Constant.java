package com.shyl.constant;


public class Constant {
//  kafka集群地址
    public static final String KAFKA_BROKERS = "nodev2001:9092,nodev2002:9092,nodev2003:9092,nodev2004:9092";
//  业务数据同步的指定topic
    public static final String TOPIC_DB = "topic_real_db";
//    日志数据同步的指定topic
    public static final String TOPIC_LOG = "topic_real_log";

//  mysql相关参数
    public static final String MYSQL_HOST = "nodev2001";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "1234kxmall!@#ABC";
//    监控的mysql数据库
    public static final String MYSQL_DATABASE = "real_gmall";

//    mysql 同步到hbase的配置库
    public static final String MYSQL2HBASE_CONFIG_DATABASE = "gmall2024_config";


//    全量同步维度表
    public static final String TABLE_LIST[] =
    {
        "real_gmall.activity_info",
        "real_gmall.activity_rule",
        "real_gmall.activity_sku",
        "real_gmall.base_category1",
        "real_gmall.base_category2",
        "real_gmall.base_category3",
        "real_gmall.base_province",
        "real_gmall.base_region",
        "real_gmall.base_trademark",
        "real_gmall.coupon_info",
        "real_gmall.coupon_range",
        "real_gmall.financial_sku_cost",
        "real_gmall.sku_info",
        "real_gmall.spu_info",
        "real_gmall.user_info",
    };


    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://nodev2001:3306?useSSL=false";

//    hbase相关参数-未修改
    public static final String HBASE_NAMESPACE = "real_gmall";
    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";

    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";

    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";

    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "dwd_trade_order_cancel";

    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "dwd_trade_order_payment_success";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";

    public static final String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "dwd_trade_refund_payment_success";

    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";

}
