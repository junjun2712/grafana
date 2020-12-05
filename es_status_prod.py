#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#
# Author:   Marathon <jsdymarathon@itcom888.com>
# Date:     
# Location: Pasay
# Desc:     通过 elasticsearch _cat/health接口获取es集群的当前状态，并提供接口给prometheus采集，最终在grafana展示

import prometheus_client
from prometheus_client import Gauge
from prometheus_client.core import CollectorRegistry
from flask import Response, Flask
import requests

app = Flask(__name__)


def get_es_cluster_health():
    """从ES _cat/health 接口获取集群状态信息
    接口返回数据说明：
        时间戳               集群名称  集群状态 在线节点总数  在线的数据节点总数  存活的分片数量  存活的主分片数量
        epoch     timestamp  cluster  status  node.total     node.data         shards         pri
        1580486697 16:04:57  prodes   green   9             3                 40             20

        迁移中的分片数量  初始化中的分片数量 未分配的分片  准备中的任务    任务最长等待时间     正常分片百分比
        relo            init              unassign      pending_tasks  max_task_wait_time active_shards_percent
        0               0                 0             0             -                   100.0%
        集群状态说明：
            green: 健康,
            yellow: 代表分配了所有主分片,但至少缺少一个副本,此时集群数据仍旧完整,
            red: 代表部分主分片不可用,可能已经丢失数据
    """
    es_health_metric_list = [
        "epoch", "timestamp", "cluster", "status", "node_total", "node_data", "shards", "pri", "relo", "init",
        "unassign", "pending_tasks", "max_task_wait_time", "active_shards_percent"
    ]
    cluster_status_map = {
        'green': 1,
        'yellow': 2,
        'red': 3
    }
    es_health_url = 'http://10.3.12.1:9200/_cat/health'
    es_user = 'java_app'
    es_pass = '123456'
    es_r = requests.get(es_health_url, auth=(es_user, es_pass))
    es_r_list = es_r.text.encode('utf8').split()
    #print(es_r_list)
    es_health_metric_dict = dict(zip(es_health_metric_list, es_r_list))
    es_health_metric_dict['status'] = cluster_status_map[es_health_metric_dict.get('status')]
    #print(es_health_metric_dict)
    return es_health_metric_dict


@app.route("/metrics")
def requests_count():
    # 获取health数据字典
    es_cluster_data = get_es_cluster_health()
    cluster_name = es_cluster_data.get('cluster')
    max_task_wait_time = 0 if es_cluster_data.get('max_task_wait_time') == '-' else es_cluster_data.get('max_task_wait_time')
    if max_task_wait_time and 's' in max_task_wait_time:
        max_task_wait_time = float(max_task_wait_time.split('s')[0])
    elif max_task_wait_time and 'm' in max_task_wait_time:
        max_task_wait_time = float(max_task_wait_time.split('m')[0]) * 60
    elif max_task_wait_time and 'h' in max_task_wait_time:
        max_task_wait_time = float(max_task_wait_time.split('h')[0]) * 60 * 60
    
    # 设置metrics的值与label
    es_cluster_status.labels(cluster_name).set(es_cluster_data.get('status'))
    es_node_total.labels(cluster_name).set(es_cluster_data.get('node_total'))
    es_node_data.labels(cluster_name).set(es_cluster_data.get('node_data'))
    es_active_shards.labels(cluster_name).set(es_cluster_data.get('shards'))
    es_active_master_shards.labels(cluster_name).set(es_cluster_data.get('pri'))
    es_relo_shards.labels(cluster_name).set(es_cluster_data.get('relo'))
    es_init_shards.labels(cluster_name).set(es_cluster_data.get('init'))
    es_unassign_shards.labels(cluster_name).set(es_cluster_data.get('unassign'))
    es_pending_tasks.labels(cluster_name).set(es_cluster_data.get('pending_tasks'))
    es_max_task_wait_time.labels(cluster_name).set(max_task_wait_time)
    es_active_shards_percent.labels(cluster_name).set(es_cluster_data.get('active_shards_percent').split('%')[0])
    
    return Response(prometheus_client.generate_latest(REGISTRY),
                    mimetype="text/plain")


if __name__ == "__main__":
    # 实例化 REGISTRY
    REGISTRY = CollectorRegistry(auto_describe=False)

    # 设置指标并添加 label
    es_cluster_status = Gauge("es_cluster_status", "prod es cluster status 1:green/2:yellow/3:red gauge", ['env'], registry=REGISTRY)
    es_node_total = Gauge("es_node_total", "es cluster total node nums gauge", ['env'], registry=REGISTRY)
    es_node_data = Gauge("es_node_data", "es cluster total data node nums gauge", ['env'], registry=REGISTRY)
    es_active_shards = Gauge("es_active_shards", "es cluster active shards nums gauge", ['env'], registry=REGISTRY)
    es_active_master_shards = Gauge("es_active_master_shards", "es cluster active master shards nums gauge", ['env'], registry=REGISTRY)
    es_relo_shards = Gauge("es_relo_shards", "es cluster relo shards nums gauge", ['env'], registry=REGISTRY)
    es_init_shards = Gauge("es_init_shards", "es cluster init shards nums gauge", ['env'], registry=REGISTRY)
    es_unassign_shards = Gauge("es_unassign_shards", "es cluster unassign shards nums gauge", ['env'], registry=REGISTRY)
    es_pending_tasks = Gauge("es_pending_tasks", "es cluster pending_tasks nums gauge", ['env'], registry=REGISTRY)
    es_max_task_wait_time = Gauge("es_max_task_wait_time", "es cluster max_task_wait_time gauge", ['env'], registry=REGISTRY)
    es_active_shards_percent = Gauge("es_active_shards_percent", "es cluster active_shards_percent gauge", ['env'], registry=REGISTRY)

    app.run(host="0.0.0.0", port=32672, debug=True)

