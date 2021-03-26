import re
import json

def check_lack_douyin(input_file,standard_file):
    idx = 0
    product_dict = {}
    B_brand_dict = {}
    A_brand_dict = {}
    sort_A_brand_dict = {}
    sort_B_brand_dict = {}
    brand_stat_dict = {}
    B_top100_product_list = []
    A_B_top100_product_list = []
    compile_num = 0
    brand_dict_num = {}
    result_list = []

    no_brand_list = []

    brand_dict,ori_brand_dict = split_standard_brand_company(standard_file)
    print(len(brand_dict))
    with open(input_file,"r",encoding="utf-8") as f1:
        for line in f1:
            flag = 0
            idx += 1
            max_len = 0
            brand_result = ''
            condidate_list = []
            if idx%10000 == 0:print(idx)
            if len(line) == 0:continue
            line_list = line.strip().split("\t")

            #对于第一个文件
            # if len(line_list) != 11:continue
            # line_list = [tmp.strip() for tmp in line_list]
            # cp_id,hospital_id,id,key_words,price,note_num,origin,reverse_num,batch,crawl_time,dt = line_list

            #对于第三个文件
            if len(line_list) != 2:continue
            line_list = [tmp.strip() for tmp in line_list]
            cp_id,key_words = line_list
            # key_words = title + " " + content

            #对于第二个文件
            # if len(line_list) != 1:continue
            # line_list = [tmp.strip() for tmp in line_list]
            # key_words = line_list[0]

            key_words = special_words_reg(key_words)

            # if compile_num < 100:
            #     print(key_words)

            for brand_id_item, brand_name_item in brand_dict.items():
                for brand_name_item_s in brand_name_item:
                    if len(key_words.split(str(brand_name_item_s))) > 1:
                        condidate_list.append(brand_id_item)
                        break

            if len(condidate_list) > 0:
                flag = 1
                compile_num += 1
                for item in condidate_list:
                    if len(ori_brand_dict[item]) > max_len:
                        max_len = len(ori_brand_dict[item])
                        brand_result = item
                    else:
                        continue

            if brand_result in brand_dict_num:
                brand_dict_num[brand_result] += 1
            else:
                brand_dict_num[brand_result] = 1

            if flag == 1:
                tmp_list = [cp_id,key_words,brand_result,ori_brand_dict[brand_result]]
            else:
                # tmp_list = [cp_id, key_words, '', '']
                continue

            result_list.append(tmp_list)

                        # if brand_id_item in brand_dict_num:
                        #     brand_dict_num[brand_id_item] += 1
                        # else:
                        #     brand_dict_num[brand_id_item] = 1
                        # # tmp_list = []
                        # break
                # if flag == 1:
                #     break

            if flag == 0:
                no_brand_list.append(key_words)

    return compile_num,brand_dict_num,no_brand_list,result_list
            # product_dict[product_id.strip()] = line_list
            # if brand_id not in B_brand_dict:
            #     B_brand_dict[brand_id] = [product_id]
            # else:
            #     B_brand_dict[brand_id].append(product_id)
    #         for brand_id_item,brand_name_item_list in brand_dict.items():
    #             for brand_name_item in brand_name_item_list:
    #                 # brand_name_item_ = re.findall('[\u4e00-\u9fa5a-zA-Z0-9]+',brand_name_item,re.S)
    #                 # brand_name_item_ = re.sub('\W+', '', brand_name_item).replace("_", '')
    #                 # if brand_id_item == '10421087':
    #                 #     print(brand_name_item + "   " + key_words)
    #                 # key_words_ = re.sub('\W+', '', key_words).replace("_", '')
    #                 if len(key_words.split(brand_name_item)) > 1:
    #
    #                     # if brand_id_item not in A_brand_dict:
    #                     #     A_brand_dict[brand_id_item] = [product_id]
    #                     # else:
    #                     #     A_brand_dict[brand_id_item].append(product_id)
    #                     break
    #
    # for item,values_list in A_brand_dict.items():
    #     sorted_list = sorted(values_list,key = lambda k:float(product_dict[k][12]),reverse=True)
    #     sort_A_brand_dict[item] = sorted_list
    #
    # for item,values_list in B_brand_dict.items():
    #     sorted_list = sorted(values_list,key = lambda k:float(product_dict[k][12]),reverse=True)
    #     sort_B_brand_dict[item] = sorted_list
    #
    #
    # for brand in ori_brand_dict:
    #     brand_name = ori_brand_dict[brand]
    #     A_gmv = 0.0
    #     A_product_num = 0
    #     B_gmv = 0.0
    #     B_product_num = 0
    #     B_top100_gmv = 0.0
    #     A_B_brand_list = []
    #     A_B_gmv = 0.0
    #     A_B_top100_gmv = 0.0
    #     B_divide_A_num = 0.0
    #     B_divide_A_gmv = 0.0
    #     B_top100_gmv_rate = 0.0
    #     A_B_top100_gmv_rate = 0.0
    #
    #     if brand in sort_A_brand_dict:
    #         A_product_num = len(sort_A_brand_dict[brand])
    #         for product_item in sort_A_brand_dict[brand]:
    #             A_gmv = A_gmv + float(product_dict[product_item][12])
    #             if brand not in sort_B_brand_dict:
    #                 A_B_brand_list = sort_A_brand_dict[brand]
    #             else:
    #                 if product_item not in sort_B_brand_dict[brand]:
    #                     A_B_brand_list.append(product_item)
    #
    #     if brand in sort_B_brand_dict:
    #         index = 0
    #         B_product_num = len(sort_B_brand_dict[brand])
    #         for product_item in sort_B_brand_dict[brand]:
    #             index += 1
    #             B_gmv = B_gmv + float(product_dict[product_item][12])
    #             if index <= 100:
    #                 B_top100_gmv = B_top100_gmv + float(product_dict[product_item][12])
    #                 B_top100_product_list.append(str(brand) + "\t" + str(brand_name) + "\t" + str(product_item) + "\t" + str(product_dict[product_item][12]) + "\t" + str(product_dict[product_item][4]) + "\t" + str(product_dict[product_item][5]) + "\t" + str(product_dict[product_item][8]) + "\t" + str(product_dict[product_item][2]) + "\t" + str(product_dict[product_item][1]))
    #
    #
    #     if len(A_B_brand_list) != 0:
    #         index = 0
    #         for product in A_B_brand_list:
    #             index += 1
    #             A_B_gmv = A_B_gmv + float(product_dict[product][12])
    #             if index <= 100:
    #                 A_B_top100_gmv = A_B_top100_gmv + float(product_dict[product][12])
    #                 A_B_top100_product_list.append(str(brand) + "\t" + str(brand_name) + "\t" + str(product) + "\t" + str(product_dict[product][12]) + "\t" + str(product_dict[product][6]) + "\t" + str(product_dict[product][7]) + "\t" + str(product_dict[product][4]) + "\t" + str(product_dict[product][5]) + "\t" + str(product_dict[product][8]) + "\t" + str(product_dict[product][2]) + "\t" + str(product_dict[product][1]))
    #
    #
    #     if A_product_num >= B_product_num and A_product_num != 0:
    #         B_divide_A_num = B_product_num/A_product_num
    #
    #     if A_gmv >= B_gmv and A_gmv != 0.0:
    #         B_divide_A_gmv = B_gmv / A_gmv
    #
    #     if B_gmv != 0.0:
    #         B_top100_gmv_rate = B_top100_gmv/B_gmv
    #     if A_B_gmv != 0.0:
    #         A_B_top100_gmv_rate = A_B_top100_gmv/A_B_gmv
    #
    #     brand_stat_dict[brand] = [brand,brand_name,A_product_num,A_gmv,B_product_num,B_gmv,B_top100_gmv,A_B_top100_gmv,B_divide_A_num,B_divide_A_gmv,B_top100_gmv_rate,A_B_top100_gmv_rate]


    # return brand_stat_dict,B_top100_product_list,A_B_top100_product_list

def special_words_reg(word_str):
    # pattern = re.sub(u"\\(.*?\\)|\\{.*?}|\\[.*?]", "", word_str)
    # pattern = re.sub(u"\\（.*?\\）|\\{.*?}|\\[.*?]", "", pattern)
    brand_name_item = re.sub('\W+', '', word_str).replace("_", '').lower()
    return brand_name_item

def split_standard_brand(standard_file):
    brand_dict = {}
    ori_brand_dict = {}
    del_words = ['美国','上海','法国','韩国','西班牙','欧洲','国产','美版','意大利','英国','日本','广州']
    with open(standard_file,"r",encoding="utf-8") as f1:
        for line in f1:
            if len(line) == 0:continue
            brand_name = line.strip()
            brand_name_list = brand_name.split("\t")
            if len(brand_name_list) != 3:continue
            variety,drug_name,company_name = brand_name_list
            # ori_brand_dict[company_name.strip()] = ''
            brand_name_item = special_words_reg(company_name)
            for del_word in del_words:
                brand_name_item = brand_name_item.replace(del_word,"")
            brand_dict[brand_name_item.strip()] = ""

    return brand_dict,ori_brand_dict


def split_standard_brand_drug(standard_file):
    brand_dict = {}
    ori_brand_dict = {}
    # del_words = ['美国','上海','法国','韩国','西班牙','欧洲','国产','美版','意大利','英国','日本','广州']
    with open(standard_file,"r",encoding="utf-8") as f1:
        for line in f1:
            if len(line) == 0:continue
            brand_name = line.strip()
            brand_name_list = brand_name.split("\t")
            if len(brand_name_list) != 3:continue
            variety,drug_name,company_name = brand_name_list
            # ori_brand_dict[company_name.strip()] = ''
            brand_name_item = special_words_reg(drug_name)
            # for del_word in del_words:
            #     brand_name_item = brand_name_item.replace(del_word,"")
            brand_dict[brand_name_item.strip()] = ""

    return brand_dict,ori_brand_dict

def split_standard_brand_company_v1(standard_file):
    brand_dict = {}
    ori_brand_dict = {}
    # del_words = ['美国','上海','法国','韩国','西班牙','欧洲','国产','美版','意大利','英国','日本','广州']
    with open(standard_file,"r",encoding="utf-8") as f1:
        for line in f1:
            if len(line) == 0:continue
            brand_name = line.strip()
            brand_name_list = brand_name.split("/")
            # ori_brand_dict[company_name.strip()] = ''
            tmp_list = []
            for brand_item in brand_name_list:
                tmp_list.append(special_words_reg(brand_item))
            # for del_word in del_words:
            #     brand_name_item = brand_name_item.replace(del_word,"")
            brand_dict[brand_name] = tmp_list

    return brand_dict,ori_brand_dict

def is_all_eng(strs):
    for _char in strs:
        if '\u4e00' <= _char <= '\u9fa5':
            return False
    return True

remove_brand_list = ['先锋','统一','博世','I Do','老板','现代','坚果','英雄','大王','钟薛高','Pico','Beats','荣耀','皇家','好奇']

def split_standard_brand_company(standard_file):
    brand_dict = {}
    ori_brand_dict = {}
    # del_words = ['美国','上海','法国','韩国','西班牙','欧洲','国产','美版','意大利','英国','日本','广州']
    with open(standard_file,"r",encoding="utf-8") as f1:
        for line in f1:
            if len(line) == 0:continue
            line_list = line.strip().split("\t")
            if len(line_list) != 5:
                print(line)
                continue
            brand_id, brand_name,cat1_id,cat1_name,gmv = line_list
            brand_name = brand_name.strip()
            if brand_id in brand_dict:continue
            brand_name_list = brand_name.split("/")
            # ori_brand_dict[company_name.strip()] = ''
            tmp_list = []
            for brand_item in brand_name_list:
                if (is_all_eng(brand_item) and len(brand_item) <= 3) or len(brand_item)  < 2 or brand_item in remove_brand_list:continue
                tmp_list.append(special_words_reg(brand_item))
            # for del_word in del_words:
            #     brand_name_item = brand_name_item.replace(del_word,"")
            if len(tmp_list) == 0:continue
            ori_brand_dict[brand_id] = brand_name
            brand_dict[brand_id] = tmp_list

    return brand_dict,ori_brand_dict

def gen_standard_cat1(input_file):
    brand_id_dict = {}
    with open(input_file,"r",encoding="utf-8") as f1:
        for line in f1:
            if len(line) == 0:continue
            line_list = line.strip().split("\t")
            if len(line_list) != 5:continue
            brand_id, brand_name, cat1_id, cat1_name, gmv = line_list
            if brand_id in brand_id_dict:
                temp_list = set(brand_id_dict[brand_id] + [cat1_name])
                brand_id_dict[brand_id] = list(temp_list)
            else:
                brand_id_dict[brand_id] = [cat1_name]
    return brand_id_dict


def check_data_deal(input_file,brand_file):
    brand_id_dict = gen_standard_cat1(brand_file)
    data_cat1_info_dict = {}
    idx = 0
    with open(input_file,"r",encoding="utf-8") as f2:
        for line in f2:
            if len(line) == 0:continue
            line_list = line.strip().split("\t")
            if len(line_list) != 4:continue
            cp_id,title,brand_id,brand_name = line_list
            data_cat1_info_dict[cp_id] = [cp_id,title,brand_id,brand_name,"|".join(brand_id_dict[brand_id])]
    with open("./bilibili_check_cat1.txt","w",encoding="utf-8") as f3:
        for k,v in data_cat1_info_dict.items():
            f3.write("\t".join(v))
            f3.write("\n")
    print(idx)


if __name__ == "__main__":
    # brand_dict,ori_brand_dict = split_standard_brand_company("z_douyin_clean_pro/standard_brand.txt")
    # print(brand_dict)

    # input_file = "./data/bilibili_ori_data.txt"
    # standard_file = "standard_brand_info.txt"
    # compile_num,brand_dict,no_brand_list,result_list = check_lack_douyin(input_file,standard_file)
    # print(compile_num)

    #统计每种药品的数量
    # brand_list = [(k,v) for k,v in brand_dict.items()]
    # brand_list_sorted = sorted(brand_list,key=lambda x:x[1],reverse=True)
    # for k,v in brand_list_sorted:
    #     print(str(k) + "\t" +str(v))

    # with open("./bilibili_results.txt","w",encoding="utf-8") as f2:
    #     for i in range(len(result_list)):
    #         f2.write("\t".join(result_list[i]))
    #         f2.write("\n")

    #未匹配出来的数据列表
    # d_flag = 0
    # for v in no_brand_list:
    #     d_flag += 1
    #     if d_flag <= 1000:
    #         print(str(v))


    # ss_dict = {'item_1':"aasdasda",'item_2':"aasdasda",'item':"中国"}
    # with open("demo_json.json","w",encoding="utf-8") as f1:
    #     json.dump(ss_dict,f1,ensure_ascii=False)

    # s_list = [11,22,33,44,55]
    # with open("demo_list.txt","w",encoding="utf-8") as f2:
    #     for item in s_list:
    #         f2.write("%s\n"%item)


    #给人工审核的数据加上一级类信息
    input_file = 'check_data.txt'
    brand_file = 'standard_brand_info.txt'
    check_data_deal(input_file,brand_file)