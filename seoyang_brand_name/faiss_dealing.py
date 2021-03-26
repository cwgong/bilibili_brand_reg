#!/usr/bin/env python3
#coding=utf-8

import sys, os
import shutil
import traceback
import logging.config
from logging.handlers import TimedRotatingFileHandler
import faiss_opt
from faiss_opt import JDCreateAndSaveFaissIndex, FaissIndexDbMerge_Ext

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    datefmt='%a, %d %b %Y %H:%M:%S',
    filemode='a'
)

log_instance = logging.getLogger("faiss_index_create_merge_logger")
log_file_name = 'log/faiss_index_create_merge_log'
fileTimeHandler = TimedRotatingFileHandler(log_file_name, \
                                           when="D", \
                                           interval=1, \
                                           backupCount=10)
fileTimeHandler.suffix = "%Y-%m-%d.log"
formatter = logging.Formatter('%(name)-12s %(asctime)s level-%(levelname)-8s thread-%(thread)-8d %(message)s')
fileTimeHandler.setFormatter(formatter)
log_instance.addHandler(fileTimeHandler)


def faiss_index_creating(faiss_saving_folder, jd_cat1_en_name):
    saving_base_folder = faiss_saving_folder
    faiss_index_saving_folder = "%s/faiss_index" % saving_base_folder
    if not os.path.exists(faiss_index_saving_folder):
        os.mkdir(faiss_index_saving_folder)
    sku_index_saving_folder = "%s/sku_index" % saving_base_folder
    if not os.path.exists(sku_index_saving_folder):
        os.mkdir(sku_index_saving_folder)
    faiss_postfix = faiss_opt.FAISS_POSTFIX # 'faiss.ivfflat.index.db'
    sku_postfix = faiss_opt.SKU_POSTFIX # 'sku.index'
    # create faiss_index_cat1_part

    log_instance.info("%s part-faiss-index creating start!" % jd_cat1_en_name)
    for vec_file_path in faiss_opt.getting_sorted_file_name_lst(vec_folder):
        try:
            fname = vec_file_path.split("/")[-1]
            faiss_index_save_path = "%s/%s.%s" % (faiss_index_saving_folder, fname, faiss_postfix)
            sku_index_save_path = "%s/%s.%s" % (sku_index_saving_folder, fname, sku_postfix)
            log_instance.info("create %s faiss index: %s, vec-data: %s" %
                              (jd_cat1_en_name, faiss_index_save_path, vec_file_path))
            build_ivfflat_faiss_index_obj = JDCreateAndSaveFaissIndex(vec_ori_data_path=vec_file_path, \
                                                                      faiss_model_save_path=faiss_index_save_path, \
                                                                      sku_idx_save_path=sku_index_save_path, \
                                                                      log_instance=log_instance, \
                                                                      is_vec_ori_data_folder=False)
            build_ivfflat_faiss_index_obj.creating_and_saving_IVFFlat()
        except Exception as e:
            log_instance.error(traceback.format_exc())
            sys.exit(1)
    log_instance.info("%s part-faiss-index creating end!\n\n" % jd_cat1_en_name)

    part_faiss_index_file_lst = faiss_opt.getting_sorted_file_name_lst(faiss_index_saving_folder)
    part_sku_index_file_lst = faiss_opt.getting_sorted_file_name_lst(sku_index_saving_folder)
    base_faiss_index_path = part_faiss_index_file_lst[0]
    base_sku_index_path = part_sku_index_file_lst[0]
    merge_faiss_index_path = "%s/%s.%s" % (saving_base_folder, jd_cat1_en_name, faiss_postfix)
    merge_sku_index_path = "%s/%s.%s" % (saving_base_folder, jd_cat1_en_name, sku_postfix)

    if len(part_faiss_index_file_lst) == 1:
        try:
            shutil.copyfile(base_faiss_index_path, merge_faiss_index_path)
            shutil.copyfile(base_sku_index_path, merge_sku_index_path)
        except Exception as e:
            log_instance.error(traceback.format_exc())
            sys.exit(1)
    else:
        # create faiss_index_cat1
        try:

            append_tuple_lst = []
            for i in range(1, len(part_faiss_index_file_lst)):
                append_tuple_lst.append((part_faiss_index_file_lst[i], part_sku_index_file_lst[i]))

            print(append_tuple_lst)
            log_instance.info("%s part-faiss-index merging start!" % jd_cat1_en_name)
            faiss_merge_obj = FaissIndexDbMerge_Ext(base_faiss_index_file=base_faiss_index_path, \
                                                    base_sku_index_file=base_sku_index_path, \
                                                    merge_faiss_index_file=merge_faiss_index_path, \
                                                    merge_sku_index_file=merge_sku_index_path, \
                                                    log_instance=log_instance)
            log_instance.info("%s part_faiss_index merging!" % jd_cat1_en_name)

            faiss_merge_obj.multi_faiss_index_merge_and_saving(append_tuple_lst)
            log_instance.info("%s part-faiss-index merging end!\n\n" % jd_cat1_en_name)
        except Exception as e:
            log_instance.error(traceback.format_exc())
            sys.exit(1)

if __name__ == "__main__":
    jd_cat1_en_name = sys.argv[1]
    vec_folder = sys.argv[2]
    faiss_saving_folder = sys.argv[3]
    if not os.path.exists(vec_folder):
        log_instance.error("%s does not exist!" % vec_folder)
        sys.exit(1)

    if not os.path.exists(faiss_saving_folder):
        log_instance.error("%s does not exist!" % faiss_saving_folder)
        sys.exit(1)

    faiss_index_creating(faiss_saving_folder, jd_cat1_en_name)