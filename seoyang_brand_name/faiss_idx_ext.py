#!/usr/bin/env python3
#coding=utf-8

import sys, os
from time import strftime, localtime
import numpy as np
import faiss
import json
from faiss import normalize_L2
import logging.config
from logging.handlers import TimedRotatingFileHandler
from multiprocessing.dummy import Pool as ThreadPool

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

def rate_limited_imap(f, l):
    'a thread pre-processes the next element'
    pool = ThreadPool(1)
    res = None
    for i in l:
        res_next = pool.apply_async(f, (i, ))
        if res:
            yield res.get()             #yield是一个迭代器返回函数，作用类似于return，不过yield会记住上次返回的索引位置，多次返回，最后在调用端的表现为一个list，该函数一般在for循环中使用，配合多进程使用。
        res = res_next                  #get方法是阻塞的。示例代码中这样做的原因是为了加一个进程，就让该进程的结果返回
    yield res.get()


def matrix_slice_iterator(x, bs):
    " iterate over the lines of x in blocks of size bs"
    nb = x.shape[0]     #x为1000行128列
    block_ranges = [(i0, min(nb, i0 + bs))      #使用该方法按bs-->50将x矩阵按行切分，若干个小块
                    for i0 in range(0, nb, bs)]

    return rate_limited_imap(
        lambda i01: x[i01[0]:i01[1]].astype('float32').copy(),      #将上方切分的多个小块儿分别加入到迭代器中处理，取到矩阵的多个小块数据，然后多线程的迭代器get方法会分别返回结果.结果格式依然还是list.
        block_ranges)



def save_index_v1(output_index_file,sku2vector_dir,output_sku_list):
    data_dimension = 768

    data = []
    sku_lst = []
    error_sku = []
    idx = 0

    log_instance.info("start upload data sku part!")
    printTime()

    f_name_lst = []
    for root, dirs, files in os.walk(sku2vector_dir):
        for f_name in files:
            if not f_name.startswith('part_'): continue
            f_name = os.path.join(root, f_name)
            f_name_lst.append(f_name)
    f_name_lst = sorted(f_name_lst)

    for f_name in f_name_lst:
        with open(f_name) as f1:
            for line in f1:
                if idx > 1200000:continue
                line = line.strip()
                if line == '': continue
                lst1 = line.split('\t')
                if len(lst1) != 2: continue
                lst1 = [tmp for tmp in lst1]
                sku, vec_str = lst1
                lst2 = vec_str.split("|")
                lst2 = [float(d1) for d1 in lst2]
                data.append(lst2)
                sku_lst.append(sku)
                if idx % 5000 == 0:
                    log_instance.info("idx: %s" % idx)
                idx += 1

    log_instance.info("end upload data sku part!")
    printTime()

    log_instance.info(len(error_sku))
    log_instance.info(len(data))

    with open(output_sku_list,"w",encoding="utf-8") as f1:
        json.dump(sku_lst,f1)

    data = np.array(data).astype('float32')
    printTime()

    log_instance.info('normalize_L2')
    normalize_L2(data)
    log_instance.info("start upload data faiss!")
    printTime()

    index = faiss.IndexFlatIP(data_dimension)
    index.train(data)
    log_instance.info(index.is_trained)
    index.add(data)

    print("write", "%s" %(output_index_file))
    printTime()
    faiss.write_index(index, output_index_file)
    log_instance.info("end write index to faiss!")
    printTime()


def save_index(output_index_file, sku2vector_dir, output_sku_list):
    data_dimension = 768

    data = []
    sku_lst = []
    error_sku = []
    idx = 0

    log_instance.info("start upload data sku part!")
    printTime()

    f_name_lst = []
    for root, dirs, files in os.walk(sku2vector_dir):
        for f_name in files:
            if not f_name.startswith('part_'): continue
            f_name = os.path.join(root, f_name)
            f_name_lst.append(f_name)
    f_name_lst = sorted(f_name_lst)

    for f_name in f_name_lst:
        with open(f_name) as f1:
            for line in f1:
                if idx > 1500000: continue
                line = line.strip()
                if line == '': continue
                lst1 = line.split('\t')
                if len(lst1) != 2: continue
                lst1 = [tmp for tmp in lst1]
                sku, vec_str = lst1
                lst2 = vec_str.split("|")
                lst2 = [float(d1) for d1 in lst2]
                data.append(lst2)
                sku_lst.append(sku)
                if idx % 5000 == 0:
                    log_instance.info("idx: %s" % idx)
                idx += 1

    log_instance.info("end upload data sku part!")
    printTime()

    log_instance.info(len(error_sku))
    log_instance.info(len(data))

    with open(output_sku_list, "w", encoding="utf-8") as f1:
        json.dump(sku_lst, f1)

    data_mat = np.mat(data)
    log_instance.info('normalize_L2')
    normalize_L2(data_mat)

    index = faiss.IndexFlatIP(data_dimension)
    index.train(data_mat)

    i0 = 0
    log_instance.info(index.is_trained)
    for xs in matrix_slice_iterator(data_mat, 100000):
        i1 = i0 + xs.shape[0]
        # print('\radd %d:%d, %.3f s' % (i0, i1, time.time() - t0), end=' ')
        # sys.stdout.flush()
        index.add(xs)
        i0 = i1

    log_instance.info("start upload data faiss!")
    printTime()

    print("write", "%s" % (output_index_file))
    printTime()
    faiss.write_index(index, output_index_file)
    log_instance.info("end write index to faiss!")
    printTime()


def test_save_idx(input_idx_file,output_test_result,query_num,sku2vector_dir = "title_out"):

    data = []
    sku_lst = []
    error_sku = []
    idx = 0

    log_instance.info("start upload data sku part!")
    printTime()

    f_name_lst = []
    for root, dirs, files in os.walk(sku2vector_dir):
        for f_name in files:
            if not f_name.startswith('part_'): continue
            f_name = os.path.join(root, f_name)
            f_name_lst.append(f_name)
    f_name_lst = sorted(f_name_lst)

    for f_name in f_name_lst:
        with open(f_name) as f1:
            for line in f1:
                line = line.strip()
                if line == '': continue
                lst1 = line.split('\t')
                if len(lst1) != 2: continue
                lst1 = [tmp for tmp in lst1]
                sku, vec_str = lst1

                lst2 = vec_str.split("|")
                lst2 = [float(d1) for d1 in lst2]
                data.append(lst2)
                sku_lst.append(sku)
                if idx % 5000 == 0:
                    log_instance.info("idx: %s" % idx)
                idx += 1

    log_instance.info("end upload data sku part!")
    printTime()

    log_instance.info(len(error_sku))
    log_instance.info(len(data))

    search_lst = data[:query_num]
    query = np.array(search_lst).astype('float32')
    normalize_L2(query)
    topN = 3

    log_instance.info('loading index from %s!' %(input_idx_file))
    printTime()
    index = faiss.read_index(input_idx_file)
    log_instance.info('ended loading index from %s!' % (input_idx_file))
    printTime()

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
            # if sku1 not in sku_name_dict:
            #     n1 = "unk"
            #     log_instance.info(sku1)
            # else:
            #     n1 = sku_name_dict[sku1]
            # if sku2 not in sku_name_dict:
            #     n2 = "unk"
            #     log_instance.info(sku2)
            # else:
            #     n2 = sku_name_dict[sku2]
            # if sku3 not in sku_name_dict:
            #     n3 = "unk"
            #     log_instance.info(sku3)
            # else:
            #     n3 = sku_name_dict[sku3]
            r_lst.append("%s\t%s|%s\t%s|%s\t%s\t%s:%s\t%s:%s" % (s1,i1,s2,i2, s3, sku1, sku2, i2, sku3, i3))

        with open(output_test_result, "w") as f2:
            f2.write("\n".join(r_lst))
            f2.flush()

    result_dealing(dis, ind, sku_lst)

    log_instance.info("finished!")

def pdd_research_idx(input_idx_file,output_test_result,query_num,sku2vector_dir,jd_check_file):

    data = []
    sku_lst = []
    error_sku = []
    idx = 0

    log_instance.info("start upload data sku part!")
    printTime()

    f_name_lst = []
    for root, dirs, files in os.walk(sku2vector_dir):
        for f_name in files:
            if not f_name.startswith('part_'): continue
            f_name = os.path.join(root, f_name)
            f_name_lst.append(f_name)
    f_name_lst = sorted(f_name_lst)

    for f_name in f_name_lst:
        with open(f_name) as f1:
            for line in f1:
                line = line.strip()
                if line == '': continue
                lst1 = line.split('\t')
                if len(lst1) != 2: continue
                lst1 = [tmp for tmp in lst1]
                sku, vec_str = lst1

                lst2 = vec_str.split("|")
                lst2 = [float(d1) for d1 in lst2]
                data.append(lst2)
                sku_lst.append(sku)
                if idx % 5000 == 0:
                    log_instance.info("idx: %s" % idx)
                idx += 1

    log_instance.info("end upload data sku part!")
    printTime()

    log_instance.info(len(error_sku))
    log_instance.info(len(data))

    search_lst = data[:query_num]
    query = np.array(search_lst).astype('float32')
    normalize_L2(query)
    topN = 3

    log_instance.info('loading index from %s!' %(input_idx_file))
    printTime()
    index = faiss.read_index(input_idx_file)
    log_instance.info('ended loading index from %s!' % (input_idx_file))
    printTime()

    log_instance.info('start query!')
    faiss.omp_set_num_threads(10)
    dis, ind = index.search(query, topN)
    printTime()
    log_instance.info('end query!')

    def result_dealing(dis, ind, sku_lst, result_warehouse_list):
        score_lst = dis.tolist()
        idx_lst = ind.tolist()
        r_lst = []

        for j in range(len(score_lst)):
            s1, s2, s3 = score_lst[j]
            i1, i2, i3 = idx_lst[j]
            sku1_pdd = sku_lst[j]
            # if sku1 not in sku_name_dict:
            #     n1 = "unk"
            #     log_instance.info(sku1)
            # else:
            #     n1 = sku_name_dict[sku1]
            # if sku2 not in sku_name_dict:
            #     n2 = "unk"
            #     log_instance.info(sku2)
            # else:
            #     n2 = sku_name_dict[sku2]
            # if sku3 not in sku_name_dict:
            #     n3 = "unk"
            #     log_instance.info(sku3)
            # else:
            #     n3 = sku_name_dict[sku3]
            r_lst.append("%s\t%s:%s\t%s:%s\t%s:%s" % (sku1_pdd,result_warehouse_list[i1],s1,result_warehouse_list[i2], s2,result_warehouse_list[i3], s3))

        with open(output_test_result, "w") as f2:
            f2.write("\n".join(r_lst))
            f2.flush()

    with open(jd_check_file,"r",encoding="utf-8") as f3:
        result_warehouse_list = json.load(f3)

    result_dealing(dis, ind, sku_lst,result_warehouse_list)

    log_instance.info("finished!")


if __name__ == "__main__":
    # #京东sku存储成索引
    output_index_file = "/home/supdev/yangjiangtao/faiss_index_build/faiss_index_db/jiayongdianqi_matrix.faiss.index"
    sku2vector_dir = "/home/supdev/yangjiangtao/faiss_index_build/jd_index_data/jiayongdianqi_vec"
    output_sku_file = "./sku_id_list.json"
    save_index(output_index_file,sku2vector_dir,output_sku_file)

    #pdd在京东库里检索
    # input_idx_file = "/home/supdev/yangjiangtao/faiss_index_build/faiss_index_db/jiayongdianqi.faiss.index"
    # output_test_result = "./pdd2jd_check_result.txt"
    # query_num = 300000
    # sku2vector_dir = "/home/supdev/yangjiangtao/faiss_index_build/pdd_search_data/z_test"
    # jd_check_file = "./sku_id_list.json"
    # pdd_research_idx(input_idx_file,output_test_result,query_num,sku2vector_dir,jd_check_file)

    #测试样例
    # x = [1,2,3,4,5,6,7]
    # with open("./tmp.json","w",encoding="utf-8") as f1:
    #     json.dump(x,f1)
    # with open("./tmp.json","r",encoding="utf-8") as f1:
    #     x_list = json.load(f1)
    # print(type(x_list))