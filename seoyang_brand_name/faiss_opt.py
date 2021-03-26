#!/usr/bin/env python3
#coding=utf-8
# 数据的加载顺序影响较大

import sys, os
from time import strftime, localtime
import numpy as np
import traceback
import shutil
import faiss
from enum import Enum
from faiss import normalize_L2
import logging.config
from logging.handlers import TimedRotatingFileHandler

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    datefmt='%a, %d %b %Y %H:%M:%S',
    filemode='a'
)

log_instance = logging.getLogger("faiss_searchlogger")
log_file_name = 'log/faiss_search_log'
fileTimeHandler = TimedRotatingFileHandler(log_file_name, \
                                           when="D", \
                                           interval=1, \
                                           backupCount=10)
fileTimeHandler.suffix = "%Y-%m-%d.log"
formatter = logging.Formatter('%(name)-12s %(asctime)s level-%(levelname)-8s thread-%(thread)-8d %(message)s')
fileTimeHandler.setFormatter(formatter)
log_instance.addHandler(fileTimeHandler)

FAISS_POSTFIX = 'faiss.ivfflat.index.db'
SKU_POSTFIX = 'sku.index'

VEC_DIMENSION = 768
NLIST = 100
FAISS_METRIC_TYPE = faiss.METRIC_INNER_PRODUCT
NPROBE = 3
TOPN = 3

def getting_sorted_file_name_lst(folder_path):
    f_name_lst = []
    for root, dirs, files in os.walk(folder_path):
        for f_name in files:
            if not f_name.startswith('part'): continue
            f_name = os.path.join(root, f_name)
            f_name_lst.append(f_name)

    f_name_lst = sorted(f_name_lst)

    return f_name_lst

class JDCreateAndSaveFaissIndex(object):
    '''
    faiss的写是不能并行处理的
    '''
    def __init__(self, vec_ori_data_path, faiss_model_save_path, sku_idx_save_path, log_instance=None, is_vec_ori_data_folder=True):
        if not os.path.exists(vec_ori_data_path):
            raise Exception("%s does not exists!" % local_folder)

        self.vec_ori_data_path = vec_ori_data_path
        self.faiss_model_save_path = faiss_model_save_path
        self.sku_idx_save_path = sku_idx_save_path
        self.log_instance = log_instance
        log_instance.info('stp0: loading data')
        if is_vec_ori_data_folder:
            self.np_data, self.sku_lst = self._loading_data_from_folder()
        else:
            self.np_data, self.sku_lst = self._loading_data_from_file()

    def _line_dealing(self, line):
        line = line.strip()
        if line == '': return None, None
        lst1 = line.split('\t')
        if len(lst1) != 2: return None, None
        lst1 = [tmp.strip() for tmp in lst1]
        sku, vec_str = lst1
        if sku == "": return None, None
        lst2 = vec_str.split("|")
        if len(lst2) != VEC_DIMENSION: return None, None
        lst2 = [float(d1) for d1 in lst2]
        return sku, lst2

    def _loading_data_from_file(self):
        data = []
        sku_lst = []
        idx = 0
        with open(self.vec_ori_data_path) as f1:
            for line in f1:
                try:
                    sku, lst2 = self._line_dealing(line)
                    if sku == None or lst2 == None: continue
                    data.append(lst2)
                    sku_lst.append(sku)
                    if idx % 10000 == 0:
                        self.log_instance.info("idx: %s" % idx)
                    idx += 1
                except:
                    self.log_instance.error(traceback.format_exc())

        return np.array(data).astype('float32'), sku_lst

    def _loading_data_from_folder(self):
        data = []
        sku_lst = []
        idx = 0

        f_name_lst = getting_sorted_file_name_lst(self.vec_ori_data_path)

        for tmp_f_name in f_name_lst:
            with open(tmp_f_name) as f1:
                for line in f1:
                    try:
                        sku, lst2 = self._line_dealing(line)
                        if sku == None or lst2 == None: continue
                        data.append(lst2)
                        sku_lst.append(sku)
                        if idx % 10000 == 0:
                            self.log_instance.info("idx: %s" % idx)
                        idx += 1
                    except:
                        self.log_instance.error(traceback.format_exc())

        return np.array(data).astype('float32'), sku_lst

    def creating_and_saving_index(self):
        try:
            log_instance.info("data rows: %s column: %s" % self.np_data.shape)
            log_instance.info('stp1: faiss normalize_L2')
            normalize_L2(self.np_data)
            log_instance.info('stp2: faiss create index')
            index = faiss.IndexFlatIP(VEC_DIMENSION)
            index.train(self.np_data)
            log_instance.info('stp3: add data to faiss')
            index.add(self.np_data)
            log_instance.info('stp4: faiss index saving')
            faiss.write_index(index, self.faiss_model_save_path )

            with open(self.sku_idx_save_path, "w") as f2:
                f2.write("\n".join(self.sku_lst))
                f2.flush()

        except Exception as e:
            self.log_instance.error(traceback.format_exc())
            raise e

    def creating_and_saving_IVFFlat(self):
        try:
            log_instance.info('stp0: loading data')
            log_instance.info("data rows: %s column: %s" % self.np_data.shape)
            log_instance.info('stp1: set faiss quantizer')
            quantizer = faiss.IndexFlatL2(VEC_DIMENSION)
            log_instance.info('stp2: init IndexIVFFlat')
            ivf_index = faiss.IndexIVFFlat(quantizer, VEC_DIMENSION, NLIST, FAISS_METRIC_TYPE)
            log_instance.info('stp3: IndexIVFFlat train')
            normalize_L2(self.np_data)
            ivf_index.train(self.np_data)
            log_instance.info('stp4: IndexIVFFlat add data')
            ivf_index.add(self.np_data)
            log_instance.info('stp5: faiss index saving')
            faiss.write_index(ivf_index, self.faiss_model_save_path)

            with open(self.sku_idx_save_path, "w") as f2:
                f2.write("\n".join(self.sku_lst))
                f2.flush()
        except Exception as e:
            self.log_instance.error(traceback.format_exc())
            raise e

class JDFaissQuery(object):
    def __init__(self, faiss_index_file, query_vec_path, idx_save_path, num_thread=8, log_instance=None, is_vec_ori_data_folder=True):
        if not os.path.exists(faiss_index_file):
            raise Exception("%s does not exist!" % faiss_index_file)
        if not os.path.exists(query_vec_path):
            raise Exception("%s does not exist!" % query_vec_path)
        self.log_instance = log_instance
        self.faiss_index_file = faiss_index_file
        self.query_vec_path = query_vec_path
        self.idx_save_path = idx_save_path
        self.num_thread = num_thread

        self.faiss_index_obj = self.getting_faiss()
        if is_vec_ori_data_folder:
            self.query_np_data, self.query_sku_dict = self._loading_query_data_from_folder()
        else:
            self.query_np_data, self.query_sku_dict = self._loading_query_data_from_file()

    def getting_faiss(self):
        try:
            log_instance.info("begin loading faiss index: %s" % self.faiss_index_file)
            index = faiss.read_index(self.faiss_index_file)
            log_instance.info("end loading faiss index")
            return index
        except Exception as e:
            self.log_instance.error(traceback.format_exc())
            raise e

    def _line_dealing(self, line):
        line = line.strip()
        if line == '': return None, None
        lst1 = line.split('\t')
        if len(lst1) != 2: return None, None
        lst1 = [tmp.strip() for tmp in lst1]
        sku, vec_str = lst1
        lst2 = vec_str.split("|")
        if len(lst2) != VEC_DIMENSION: return None, None
        lst2 = [float(d1) for d1 in lst2]
        return sku, lst2

    def _loading_query_data_from_file(self):
        data = []
        sku_dict = {}
        idx = 0
        with open(self.query_vec_path) as f1:
            for line in f1:
                try:
                    sku, lst2 = self._line_dealing(line)
                    if sku == None or lst2 == None: continue
                    data.append(lst2)
                    sku_dict[idx] = sku
                    if idx % 10000 == 0:
                        self.log_instance.info("loading query_vec idx: %s" % idx)
                    idx += 1
                except:
                    self.log_instance.error(traceback.format_exc())

        return np.array(data).astype('float32'), sku_dict

    def _loading_query_data_from_folder(self):
        data = []
        sku_dict = {}
        idx = 0
        f_name_lst = getting_sorted_file_name_lst(self.query_vec_path)

        for f_name in f_name_lst:
            with open(f_name) as f1:
                for line in f1:
                    try:
                        sku, lst2 = self._line_dealing(line)
                        if sku == None or lst2 == None: continue
                        data.append(lst2)
                        sku_dict[idx] = sku
                        if idx % 10000 == 0:
                            self.log_instance.info("loading query_vec idx: %s" % idx)
                        idx += 1
                    except:
                        self.log_instance.error(traceback.format_exc())

        return np.array(data).astype('float32'), sku_dict

    def _search_result_dealing(self, dis, ind):
        score_lst = dis.tolist()
        idx_lst = ind.tolist()
        r_lst = []
        for j in range(len(score_lst)):
            if j not in self.query_sku_dict: continue
            s1, s2, s3 = score_lst[j]
            i1, i2, i3 = idx_lst[j]
            q_sku = self.query_sku_dict[j]
            r_lst.append("%s\t%s:%s\t%s:%s\t%s:%s" % (q_sku, i1, s1, i2, s2, i3, s3))
        return r_lst

    def _faiss_search(self, topn=TOPN):
        log_instance.info("query-data rows:%s column:%s " % self.query_np_data.shape)
        log_instance.info("num_threads: %s, topn: %s" % (self.num_thread, topn))

        log_instance.info("begin query")
        faiss.omp_set_num_threads(self.num_thread)
        normalize_L2(self.query_np_data)
        dis, ind = self.faiss_index_obj.search(self.query_np_data, topn)
        log_instance.info("end query")

        return dis, ind

    def faiss_search_and_saving(self):
        try:
            dis, ind = self._faiss_search()
            r_lst = self._search_result_dealing(dis, ind)
            with open(self.idx_save_path, "w") as f1:
                f1.write("\n".join(r_lst))
                f1.flush()

        except Exception as e:
            self.log_instance.error(traceback.format_exc())
            raise e

    def faiss_search_and_detail_info_saving(self, db_sku_idx_file, query_result_detail_file, topn=TOPN):
        dis, ind = self._faiss_search(topn)
        self._query_result_detail_info(dis, ind, db_sku_idx_file, query_result_detail_file)

    def _query_result_detail_info_v1(self, dis, ind, db_sku_idx_file, query_result_detail_file):
        try:
            idx_sku_dict = {}
            t = 0
            with open(db_sku_idx_file) as f1:
                for line in f1:
                    try:
                        sku = line.strip()
                        if line == '': continue
                        idx_sku_dict[t] = sku
                        if t % 10000 == 0:
                            log_instance.info("idx-sku: %s" % t)
                        t += 1
                    except:
                        log_instance.error(traceback.format_exc())

            score_lst = dis.tolist()
            idx_lst = ind.tolist()
            r_lst1 = []
            r_lst2 = []
            for j in range(len(score_lst)):
                if j not in self.query_sku_dict: continue
                q_sku = self.query_sku_dict[j] if j in self.query_sku_dict else "unk0"
                s1, s2, s3 = score_lst[j]
                i1, i2, i3 = idx_lst[j]
                n1 = idx_sku_dict[i1] if i1 in idx_sku_dict else "unk1"
                n2 = idx_sku_dict[i2] if i1 in idx_sku_dict else "unk2"
                n3 = idx_sku_dict[i3] if i1 in idx_sku_dict else "unk3"
                r_lst1.append("%s\t%s:%s\t%s:%s\t%s:%s" % (q_sku, i1, s1, i2, s2, i3, s3))
                r_lst2.append("%s,%s:%s,%s:%s,%s:%s" % (q_sku, n1, s1, n2, s2, n3, s3))

            with open(self.idx_save_path, "w") as f2:
                f2.write("\n".join(r_lst1))
                f2.flush()

            with open(query_result_detail_file, "w") as f3:
                f3.write("\n".join(r_lst2))
                f3.flush()

        except Exception as e:
            self.log_instance.error(traceback.format_exc())
            raise e

    def _query_result_detail_info(self, dis, ind, db_sku_idx_file, query_result_detail_file):
        try:
            idx_sku_dict = {}
            t = 0
            with open(db_sku_idx_file) as f1:
                for line in f1:
                    try:
                        sku = line.strip()
                        if line == '': continue
                        idx_sku_dict[t] = sku
                        if t % 10000 == 0:
                            log_instance.info("idx-sku: %s" % t)
                        t += 1
                    except:
                        log_instance.error(traceback.format_exc())

            score_lst = dis.tolist()
            idx_lst = ind.tolist()
            r_lst1 = []
            r_lst2 = []
            for j in range(len(score_lst)):
                if j not in self.query_sku_dict: continue
                q_sku = self.query_sku_dict[j] if j in self.query_sku_dict else "unk0"

                sku_lst = []
                for z3 in idx_lst[j]:
                    if z3 not in idx_sku_dict:
                        sku_id = 'unk'
                    else:
                        sku_id = idx_sku_dict[z3]

                    sku_lst.append(sku_id)

                tmp_lst1 = []
                tmp_lst2 = []
                for i in range(len(sku_lst)):
                    tmp_score = str(score_lst[j][i])
                    tmp_sku = sku_lst[i]
                    tmp_idx = idx_lst[j][i]
                    tmp_lst1.append("%s:%s" % (tmp_sku, tmp_score))
                    tmp_lst2.append("%s:%s" % (tmp_idx, tmp_score))

                r_lst1.append("%s\t%s" % (q_sku, ",".join(tmp_lst1)))
                r_lst2.append("%s\t%s" % (q_sku, ",".join(tmp_lst2)))

            with open(self.idx_save_path, "w") as f2:
                f2.write("\n".join(r_lst1))
                f2.flush()

            with open(query_result_detail_file, "w") as f3:
                f3.write("\n".join(r_lst2))
                f3.flush()

        except Exception as e:
            self.log_instance.error(traceback.format_exc())
            raise e

    def _IVFFlat_search(self):
        log_instance.info("query-data rows:%s column:%s " % self.query_np_data.shape)
        log_instance.info("num_threads: %s, topn: %s" % (self.num_thread, TOPN))

        log_instance.info("begin ivfflat query")
        faiss.omp_set_num_threads(self.num_thread)
        self.faiss_index_obj.nprobe = NPROBE
        normalize_L2(self.query_np_data)
        dis, ind = self.faiss_index_obj.search(self.query_np_data, TOPN)
        log_instance.info("end ivfflat query")

        return dis, ind

    def IVFFlat_search_and_saving(self):
        try:
            dis, ind = self._IVFFlat_search()
            r_lst = self._search_result_dealing(dis, ind)
            with open(self.idx_save_path, "w") as f1:
                f1.write("\n".join(r_lst))
                f1.flush()

        except Exception as e:
            self.log_instance.error(traceback.format_exc())
            raise e

    def IVFFlat_search_and_detail_info_saving(self, db_sku_idx_file, query_result_detail_file):
        dis, ind = self._IVFFlat_search()
        self._query_result_detail_info(dis, ind, db_sku_idx_file, query_result_detail_file)

class FaissIndexDbMerge_Ext(object):
    def __init__(self, base_faiss_index_file, base_sku_index_file, \
                 merge_faiss_index_file, merge_sku_index_file, log_instance=None):
        if not os.path.exists(base_faiss_index_file):
            raise Exception("%s does not exist!" % base_faiss_index_file)
        if not os.path.exists(base_sku_index_file):
            raise Exception("%s does not exist!" % base_sku_index_file)
        self.log_instance = log_instance
        self.base_faiss_obj, self.base_sku_lst = self._getting_faiss_pair(base_faiss_index_file, \
                                                                          base_sku_index_file)
        self.merge_faiss_index_file = merge_faiss_index_file
        self.merge_sku_index_file = merge_sku_index_file

    def _getting_one_faiss(self, faiss_index_file):
        try:
            index_obj = faiss.read_index(faiss_index_file)
            self.log_instance.info("loading faiss_index: %s, ntotal: %s" % (faiss_index_file, index_obj.ntotal))
            return index_obj
        except Exception as e:
            raise e

    def _getting_one_sku_index(self, sku_index_file):
        r_lst = []
        with open(sku_index_file) as f1:
            for line in f1:
                sku = line.strip()
                if sku == "": continue
                r_lst.append(sku)
        self.log_instance.info("loading sku_index: %s, sku-num: %s" % (sku_index_file, len(r_lst)))

        return r_lst

    def _getting_faiss_pair(self, faiss_index_file, sku_index_file):
        if not os.path.exists(faiss_index_file):
            raise Exception("%s does not exist!" % faiss_index_file)
        if not os.path.exists(sku_index_file):
            raise Exception("%s does not exist!" % sku_index_file)
        return self._getting_one_faiss(faiss_index_file), \
               self._getting_one_sku_index(sku_index_file)

    def two_faiss_index_merge(self, append_faiss_index_file, append_sku_index_file):
        try:
            append_index_obj, append_sku_lst = self._getting_faiss_pair(append_faiss_index_file, \
                                                                    append_sku_index_file)
            self.log_instance.info("append-faiss-index-ntotal: %s" % append_index_obj.ntotal)
            add_id = self.base_faiss_obj.ntotal
            self.base_faiss_obj.merge_from(append_index_obj, add_id)
            self.log_instance.info("append-sku-index-length: %s" % len(append_sku_lst))
            self.base_sku_lst += append_sku_lst
        except Exception as e:
            raise e

    def two_faiss_index_merge_only(self, append_faiss_index_file):
        try:
            append_index_obj = self._getting_one_faiss(append_faiss_index_file)
            add_id = self.base_faiss_obj.ntotal
            self.base_faiss_obj.merge_from(append_index_obj, add_id)
        except Exception as e:
            raise e

    def two_sku_index_merge_only(self, append_sku_index_file):
        try:
            append_sku_lst = self._getting_one_sku_index(append_sku_index_file)
            self.base_sku_lst += append_sku_lst
        except Exception as e:
            raise e

    def multi_faiss_index_merge(self, merge_lst=[]):
        """
        :param merge_lst: [(faiss_index_file, sku_index_file), ....]
        :return:
        """
        for file_tupe in merge_lst:
            try:
                faiss_index_file, sku_index_file = file_tupe
                self.log_instance.info("merge faiss_index: %s" % faiss_index_file)
                self.two_faiss_index_merge(faiss_index_file, sku_index_file)
            except Exception as e:
                self.log_instance.error(traceback.format_exc())
                raise e

    def merge_faiss_saving(self):
        try:
            self.log_instance.info("merged faiss_index_ntotal: %s" % self.base_faiss_obj.ntotal)
            faiss.write_index(self.base_faiss_obj, self.merge_faiss_index_file)
            self.log_instance.info("merged sku_index_length: %s" % len(self.base_sku_lst))
            with open(self.merge_sku_index_file, "w") as f2:
                f2.write("\n".join(self.base_sku_lst))
                f2.flush()

        except Exception as e:
            self.log_instance.error(traceback.format_exc())
            raise e

    def multi_faiss_index_merge_and_saving(self, merge_lst=[]):
        """
        stp1: 对faiss_index进行合并操作
        stp2: 对faiss_index对应的sku进行合并
        :param merge_lst: [(faiss_index_file, sku_index_file), ....]
        :return:
        """
        self.log_instance.info("only faiss_index merging!")
        for file_tupe in merge_lst:
            try:
                faiss_index_file, _ = file_tupe
                self.two_faiss_index_merge_only(faiss_index_file)
            except Exception as e:
                self.log_instance.error(traceback.format_exc())
                raise e
        faiss.write_index(self.base_faiss_obj, self.merge_faiss_index_file)
        del self.base_faiss_obj
        self.base_faiss_obj = None
        self.log_instance.info("only sku_index merging!")
        for file_tupe in merge_lst:
            try:
                _, sku_index_file = file_tupe
                self.two_sku_index_merge_only(sku_index_file)
            except Exception as e:
                self.log_instance.error(traceback.format_exc())
                raise e

        with open(self.merge_sku_index_file, "w") as f2:
            f2.write("\n".join(self.base_sku_lst))
            f2.flush()

class FaissIndexDbMerge_MemoryAdapt(object):
    def __init__(self, cat1_en_name, log_instance=None):
        self.faiss_index_base_folder = "faiss_index_db/%s" % cat1_en_name
        self.faiss_index_folder = "%s/faiss_index" % self.faiss_index_base_folder
        self.sku_index_folder = "faiss_index_db/%s/sku_index" % cat1_en_name
        if not os.path.exists(self.faiss_index_folder):
            raise Exception("%s does not exsit!" % self.faiss_index_folder)
        if not os.path.exists(self.sku_index_folder):
            raise Exception("%s does not exsit!" % self.sku_index_folder)

        self.cat1_en_name = cat1_en_name
        self.log_instance = log_instance
        # 加载faiss_index后还要进行大量vec检索，因此需要留出足够的内存空间
        self.MAX_MEMORY_SIZE = 1.0 * 1300 / 1024 # 30 * 1024 # 兆
        self.merge_basic_folder = self.faiss_index_base_folder
        self.part_merge_lst = self.faiss_memory_adapt()

    def _getting_one_faiss(self, faiss_index_file):
        try:
            index_obj = faiss.read_index(faiss_index_file)
            self.log_instance.info("loading faiss_index: %s, ntotal: %s" % (faiss_index_file, index_obj.ntotal))
            return index_obj
        except Exception as e:
            raise e

    def _getting_one_sku_index(self, sku_index_file):
        r_lst = []
        with open(sku_index_file) as f1:
            for line in f1:
                sku = line.strip()
                if sku == "": continue
                r_lst.append(sku)
        self.log_instance.info("loading sku_index: %s, sku-num: %s" % (sku_index_file, len(r_lst)))

        return r_lst

    def _getting_all_part_faiss_size(self):
        t_memory = 0.0
        t = 0
        part_faiss_index_lst = getting_sorted_file_name_lst(self.faiss_index_folder)
        self.log_instance.info("%s part_faiss_index_lst: %s" % (self.cat1_en_name, part_faiss_index_lst))

        for part_faiss_index_path in part_faiss_index_lst:
            fsize = os.path.getsize(part_faiss_index_path)
            fsize = fsize / float(1024 * 1024)  # 兆
            t_memory += fsize
            t += 1

        return t_memory, t, part_faiss_index_lst

    def faiss_memory_adapt(self):
        faiss_mem_total, files_num, part_faiss_index_lst = self._getting_all_part_faiss_size()
        if faiss_mem_total <= self.MAX_MEMORY_SIZE:
            part_num = 1
            files_num = 1
        else:
            part_num = int(faiss_mem_total / self.MAX_MEMORY_SIZE) + 1
            if part_num > files_num: part_num = files_num

        self.log_instance.info("%s faiss_index_file_num: %s" % (self.cat1_en_name, files_num))
        self.log_instance.info("%s faiss_index_total_size: %s" % (self.cat1_en_name, faiss_mem_total))
        self.log_instance.info("%s merging_part_total_num: %s" % (self.cat1_en_name, part_num))

        # part-00001.faiss.ivfflat.index.db
        lst1 = [x for x in range(files_num)]
        if part_num != 1:
            arr1 = np.array(lst1)
            split_arr = np.array_split(arr1, part_num, axis=0)
            split_lst =  []
            for tmp in split_arr:
                split_lst.append(tmp.tolist())
        else:
            split_lst = [lst1]

        merge_tupe_lst = []
        for tmp_lst in split_lst:
            z_lst = []
            for tmp_idx in tmp_lst:
                # xx/faiss_index_db/xx/faiss_index/part-00000.faiss.ivfflat.index.db
                tmp_faiss_path = part_faiss_index_lst[tmp_idx]

                fname = tmp_faiss_path.split('/')[-1]
                fname = fname.replace('.faiss.ivfflat.index.db', '')
                fname = fname.strip()
                # xx/faiss_index_db/xx/sku_index/part-00000.sku.index
                tmp_sku_path = "%s/%s.sku.index" % (self.sku_index_folder, fname)

                if not os.path.exists(tmp_faiss_path):
                    raise Exception("%s does not exists!" % tmp_faiss_path)

                if not os.path.exists(tmp_sku_path):
                    raise Exception("%s does not exists!" % tmp_sku_path)
                z_lst.append((tmp_faiss_path, tmp_sku_path))
            merge_tupe_lst.append(z_lst)

        return merge_tupe_lst

    def two_faiss_index_merge_only(self, base_faiss_obj, append_faiss_index_file):
        try:
            append_index_obj = self._getting_one_faiss(append_faiss_index_file)
            add_id = base_faiss_obj.ntotal
            base_faiss_obj.merge_from(append_index_obj, add_id)
            return base_faiss_obj
        except Exception as e:
            raise e

    def two_sku_index_merge_only(self, base_sku_lst, append_sku_index_file):
        try:
            append_sku_lst = self._getting_one_sku_index(append_sku_index_file)
            base_sku_lst += append_sku_lst
            return base_sku_lst
        except Exception as e:
            raise e

    def one_part_faiss_index_merge_and_saving(self, merge_lst=[], part_no=""):
        """
        stp1: 对faiss_index进行合并操作
        stp2: 对faiss_index对应的sku进行合并
        :param merge_lst: [[(faiss_index_file, sku_index_file), ....], [(faiss_index_file, sku_index_file), ....], ...]
        :return:
        """
        if part_no == "":
            raise Exception("part_no is empty!")
        if len(merge_lst) < 1:
            raise Exception("merge_lst is empty!")

        # getting base-faiss-file and base-sku-file
        base_faiss_index_path = merge_lst[0][0]
        base_sku_index_path = merge_lst[0][1]


        merge_faiss_index_file = "%s/%s.%s.%s" % (self.merge_basic_folder, self.cat1_en_name, part_no, FAISS_POSTFIX)
        merge_sku_index_file = "%s/%s.%s.%s" % (self.merge_basic_folder, self.cat1_en_name, part_no, SKU_POSTFIX)
        self.log_instance.info("faiss_index_merging_saving: %s" % merge_faiss_index_file)
        self.log_instance.info("sku_index_merging_saving: %s" % merge_sku_index_file)

        if len(merge_lst) == 1:
            try:
                shutil.copyfile(base_faiss_index_path, merge_faiss_index_file)
                shutil.copyfile(base_sku_index_path, merge_sku_index_file)
            except Exception as e:
                log_instance.error(traceback.format_exc())
                raise e
            return

        base_faiss_index_obj = self._getting_one_faiss(base_faiss_index_path)
        base_sku_index_lst = self._getting_one_sku_index(base_sku_index_path)
        tmp_merge_lst = merge_lst[1:]

        self.log_instance.info("only faiss_index merging!")
        for file_tupe in tmp_merge_lst:
            try:
                faiss_index_file, _ = file_tupe
                self.two_faiss_index_merge_only(base_faiss_index_obj,faiss_index_file)
            except Exception as e:
                self.log_instance.error(traceback.format_exc())
                raise e
        faiss.write_index(base_faiss_index_obj, merge_faiss_index_file)
        del base_faiss_index_obj

        self.log_instance.info("only sku_index merging!")
        for file_tupe in tmp_merge_lst:
            try:
                _, sku_index_file = file_tupe
                self.two_sku_index_merge_only(base_sku_index_lst, sku_index_file)
            except Exception as e:
                self.log_instance.error(traceback.format_exc())
                raise e

        with open(merge_sku_index_file, "w") as f2:
            f2.write("\n".join(base_sku_index_lst))
            f2.flush()
        del base_sku_index_lst

    def multi_parts_faiss_index_merging_and_saving(self):
        for idx in range(len(self.part_merge_lst)):
            part_merge_lst = self.part_merge_lst[idx]
            part_no = "part%s" % idx
            try:
                self.one_part_faiss_index_merge_and_saving(part_merge_lst, part_no)
            except Exception as e:
                raise e

class AttachJdCat4Info2TmallSku(object):
    """
    为天猫的【食品饮料】的商品获取四级类目信息
    """
    def __init__(self, jd_cat4_cat1_en_name, tmall_cat1_en_name, log_instance, topN=3):
        self.query_result_folder = "faiss_search_result"
        self.sku_info_folder = "sku_info"
        self.attach_sku_info_result = "attach_sku_info"
        self.log_instance = log_instance
        self.tmall_cat1_en_name = tmall_cat1_en_name
        self.topN = topN

        jd_sku_info_folder = "%s/%s/%s" % (self.sku_info_folder, "jd", jd_cat4_cat1_en_name)
        if not os.path.exists(jd_sku_info_folder):
            raise Exception("%s does not exist!" % jd_sku_info_folder)
        tmall_sku_info_folder = "%s/%s/%s" % (self.sku_info_folder, "tmall", tmall_cat1_en_name)
        if not os.path.exists(tmall_sku_info_folder):
            raise Exception("%s does not exist!" % tmall_sku_info_folder)

        tmall_query_result_folder = "%s/%s/%s" % (self.query_result_folder, 'tmall', tmall_cat1_en_name)
        if not os.path.exists(tmall_query_result_folder):
            raise Exception("%s does not exist!" % tmall_query_result_folder)

        tmp_query_result_folder = "%s/%s/%s" % ( self.query_result_folder, "tmall", tmall_cat1_en_name)
        if not os.path.exists(tmp_query_result_folder):
            raise Exception("%s does not exist!" % tmp_query_result_folder)

        self.jd_sku_info_dict = self._loading_jd_cat4_sku_info(jd_sku_info_folder)
        self.tmall_sku_info_dict = self._loading_tmall_sku_info(tmall_sku_info_folder)
        if len(self.jd_sku_info_dict) == 0:
            raise Exception("jd-cat4-info is empty! folder: %s" % jd_sku_info_folder)
        if len(self.tmall_sku_info_dict) == 0:
            raise Exception("jd-cat4-info is empty! folder: %s" % tmall_sku_info_folder)

        self.tmp_query_result_folder = tmp_query_result_folder

    def _loading_jd_cat4_sku_info(self, data_folder):
        file_lst = getting_sorted_file_name_lst(data_folder)
        r_dict = {}
        idx = 0
        for fname in file_lst:
            fname = fname.split("/")[-1]
            f_path = data_folder + "/" + fname
            if not os.path.exists(f_path):
                self.log_instance.error("%s file does not exists!" % f_path)
                continue
            with open(f_path) as f1:
                for line in f1:
                    line = line.strip()
                    if line == "": continue
                    lst1 = line.split("\t")
                    if len(lst1) != 4: continue
                    idx += 1
                    if idx % 100000 == 0:
                        self.log_instance.info("loading jd-cat4-sku-info: %s" % idx)
                    lst1 = [tmp.strip() for tmp in lst1]
                    sku, title, cat4_id, cat4_name = lst1
                    r_dict[sku] = (title, cat4_id, cat4_name)
        return r_dict

    def _loading_tmall_sku_info(self, data_folder):
        file_lst = getting_sorted_file_name_lst(data_folder)
        r_dict = {}
        idx = 0
        for fname in file_lst:
            fname = fname.split("/")[-1]
            f_path = data_folder + "/" + fname
            if not os.path.exists(f_path):
                self.log_instance.error("%s file does not exists!" % f_path)
                continue
            with open(f_path) as f1:
                for line in f1:
                    line = line.strip()
                    if line == "": continue
                    lst1 = line.split("\t")
                    if len(lst1) != 2: continue
                    idx += 1
                    if idx % 100000 == 0:
                        self.log_instance.info("loading tmall-sku-info: %s" % idx)
                    lst1 = [tmp.strip() for tmp in lst1]
                    sku, title = lst1
                    r_dict[sku] = title
        return r_dict

    def _attach_sku_info(self, itm_lst):
        r_lst = []
        t_sku, re_sku = itm_lst
        if t_sku not in self.tmall_sku_info_dict:
            t_title = ""
        else:
            t_title = self.tmall_sku_info_dict[t_sku]
        idx  = 0
        for tmp in re_sku.strip().split(','):
            tmp = tmp.strip()
            if tmp == "": continue
            lst1 = tmp.split(":")
            if len(lst1) != 2: continue

            lst1 = [z.strip() for z in lst1]
            j_sku, j_score = lst1
            if j_sku not in self.jd_sku_info_dict:
                continue
            idx += 1
            j_title, jd_cat4_id, jd_cat4_name = self.jd_sku_info_dict[j_sku]
            # tmall_sku tmall_title jd_sku1 jd_title1 score1 idx1
            r_lst.append("%s\001%s\001%s\001%s\001%s\001%s\001%s\001%s" % \
                         (t_sku, t_title, j_sku, j_title, jd_cat4_id, jd_cat4_name, j_score, idx))

        return r_lst

    def attach_sku_info(self):
        file_lst = getting_sorted_file_name_lst(self.tmp_query_result_folder)
        r_lst = []
        idx = 1
        for fname in file_lst:
            fname = fname.split("/")[-1]
            if not fname.endswith("_idx.result"): continue
            f_path = self.tmp_query_result_folder + "/" + fname
            if not os.path.exists(f_path):
                self.log_instance.error("%s file does not exists!" % f_path)
                continue
            self.log_instance.info("reading faiss_sku_result file: %s" % f_path)
            with open(f_path) as f1:
                for line in f1:
                    line = line.strip()
                    if line == "": continue
                    lst1 = line.split("\t")
                    if len(lst1) != 2: continue
                    idx += 1
                    if idx % 100000 == 0:
                        self.log_instance.info("attach-sku-info: %s" % idx)
                    lst1 = [tmp.strip() for tmp in lst1]
                    tmp_lst = self._attach_sku_info(lst1)
                    r_lst += tmp_lst
            out_f1 = self.attach_sku_info_result + "/tmall"
            if not os.path.exists(out_f1):
                os.mkdir(out_f1)
            out_f2 = out_f1 + "/" + self.tmall_cat1_en_name
            if not os.path.exists(out_f2):
                os.mkdir(out_f2)
            out_file = out_f2 + "/" + fname
            with open(out_file, "w") as f2:
                f2.write("\n".join(r_lst))
                f2.flush()