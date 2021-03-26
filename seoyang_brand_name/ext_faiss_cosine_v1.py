#!/usr/bin/env python3
#coding=utf-8

import sys, os
from time import strftime, localtime
import numpy as np
import faiss
from faiss import normalize_L2
import logging.config
from logging.handlers import TimedRotatingFileHandler

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    datefmt='%a, %d %b %Y %H:%M:%S',
    filemode='a'
)

log_instance = logging.getLogger("faiss_searchlogger")
log_file_name = 'faiss_search_log'
fileTimeHandler = TimedRotatingFileHandler(log_file_name, \
                                           when="D", \
                                           interval=1, \
                                           backupCount=10)
fileTimeHandler.suffix = "%Y-%m-%d.log"
formatter = logging.Formatter('%(name)-12s %(asctime)s level-%(levelname)-8s thread-%(thread)-8d %(message)s')
fileTimeHandler.setFormatter(formatter)
log_instance.addHandler(fileTimeHandler)

def printTime():
    print(strftime("%Y-%m-%d %H:%M:%S", localtime()))

data_dimension = 768

sku_name_dict = {}
log_instance.info("start upload pro data!")
printTime()
with open("p_name.txt") as f1:
    for line in f1:
        line = line.strip()
        if line == "": continue
        lst1 = line.split('\t')
        if len(lst1) != 2: continue
        lst1 = [tmp.strip() for tmp in lst1]
        sku, name = lst1
        sku_name_dict[sku] = name

log_instance.info("end upload pro data!")
printTime()

data = []
search_lst = []
sku_lst = []
error_sku = []
idx = 0

log_instance.info("start upload data sku part!")
printTime()
for root,dirs,files in os.walk("title_out"):
    for f_name in files:
        if not f_name.startswith('part_'): continue
        f_name = os.path.join(root, f_name)
        log_instance.info(f_name)
        with open(f_name) as f1:
            for line in f1:
                line = line.strip()
                if line == '': continue
                lst1 = line.split('\t')
                if len(lst1) != 2: continue
                lst1 = [tmp for tmp in lst1]
                sku, vec_str = lst1
                if sku not in sku_name_dict:
                    #print("error-sku: %s" % sku)
                    error_sku.append(sku)
                    continue
                lst2 = vec_str.split("|")
                lst2 = [float(d1) for d1 in lst2]
                data.append(lst2)
                sku_lst.append(sku)
                if idx % 5000 == 0:
                    log_instance.info("idx: %s" % idx)
                idx += 1

log_instance.info("end upload data sku part!")
printTime()

search_lst = data[:900000]

log_instance.info(len(error_sku))
log_instance.info(len(data))

data = np.array(data).astype('float32')
printTime()

log_instance.info('normalize_L2')
normalize_L2(data)
log_instance.info("start upload data faiss!")
printTime()

# index = faiss.IndexFlatIP(data_dimension)
# index.train(data)
# log_instance.info(index.is_trained)
# index.add(data)

print("loading", "jiadian_populated.index")
index = faiss.read_index("jiadian_populated.index")

log_instance.info(index.ntotal)
log_instance.info("end upload data faiss!")
printTime()

query = np.array(search_lst).astype('float32')
normalize_L2(query)
topN = 3

log_instance.info('start query!')
dis, ind = index.search(query, topN)
printTime()
log_instance.info('end query!')

def result_dealing(dis, ind, sku_lst):
    score_lst = dis.tolist()
    idx_lst = ind.tolist()
    r_lst = []
    for j in range(len(score_lst)):
        s1, s2, s3 = score_lst[j]
        i1, i2, i3 = idx_lst[j]
        sku1, sku2, sku3 = sku_lst[i1], sku_lst[i2], sku_lst[i3]
        if sku1 not in sku_name_dict:
            n1 = "unk"
            log_instance.info(sku1)
        else:
            n1 = sku_name_dict[sku1]
        if sku2 not in sku_name_dict:
            n2 = "unk"
            log_instance.info(sku2)
        else:
            n2 = sku_name_dict[sku2]
        if sku3 not in sku_name_dict:
            n3 = "unk"
            log_instance.info(sku3)
        else:
            n3 = sku_name_dict[sku3]
        r_lst.append("%s\t%s|%s\t%s|%s\t%s\t%s:%s\t%s:%s" % (n1, n2, s2, n3, s3, sku1, sku2, i2, sku3, i3))

    with open("result.txt", "w") as f2:
        f2.write("\n".join(r_lst))
        f2.flush()

result_dealing(dis, ind, sku_lst)

log_instance.info("finished!")
