import re
import json
import time
import pandas as pd
import dateutil.parser
import numpy
import os
import base64
import sys

from datetime import datetime
from dateutil.relativedelta import relativedelta
from flask import current_app, render_template
from flask_jwt import jwt_required, current_identity

from rest.exceptions import BadRequest, RestException
from rest.helpers import mysql, date_range
from rest.helpers.validator import safe_format, safe_invalid_format_message
from rest.models import CustomerModel, UserModel, BranchesModel, DivisionModel, \
    CisangkanCustomerModel, CisangkanDeliveryPlanModel, CisangkanUserModel, CisangkanVisitPlanSummaryModel, CisangkanPackingSlipModel

__author__ = 'iswan'

# dibuat sesuai keperluan
#constant
API_KEY = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJuYmYiOjE1NzQwNjUwODQsImV4cCI6MTU3NDY2OTg4NCwiaWRlbnRpdHkiOjEsImlhdCI6MTU3NDA2NTA4NH0.pC9h5aOYLjhq7FX_XxIV-MOfhwOG3zUfwjcln35qCdY"


class CisangkanController(object):
    def __init__(self):
        self.cursor = mysql.connection.cursor()
        self.customer_model = CustomerModel()
        self.user_model = UserModel()
        self.cisangkan_customer_model = CisangkanCustomerModel()
        self.cisangkan_delivery_plan_Model = CisangkanDeliveryPlanModel()
        self.cisangkan_user_model = CisangkanUserModel()
        self.cisangkan_visit_plan_summary_model = CisangkanVisitPlanSummaryModel()
        self.cisangkan_packing_slip_model = CisangkanPackingSlipModel()


    def valid_api_key(self, api_key):
        return API_KEY == api_key


    def request_credential(self, identity):
        b = True
        if(identity["username"] != "apps"): b = False
        if(identity["api_key"] != API_KEY): b = False
        return b


    def update_coordinate_customer(self, customer_data: 'dict', _id: 'int'):
        try:
            result = self.cisangkan_customer_model.update_coordinate_by_id(self.cursor, customer_data)
            mysql.connection.commit()
        except Exception as e:
            raise BadRequest(e, 200, 1)
        return result


    def update_coordinate_delivery_plan(self, str_coordinate: 'string'):
        try:
            result = self.cisangkan_delivery_plan_Model.update_coordinate_delivery_plan(self.cursor, str_coordinate)
            mysql.connection.commit()
        except Exception as e:
            raise BadRequest(e, 200, 1)
        return result


    def get_total_mycustomer(self, username: str):
        data = {}
        user = self.cisangkan_user_model.m_get_mycustomer_by_username(self.cursor, username)
        if (len(user) == 0) or (user == None):
            raise BadRequest("This User not exist", 200, 1)
        else:
            customers_dict = json.loads(user[0]['customer_id'])
            total = customers_dict.__len__()
            if(total != 0):
                max = 25
                cm = CustomerModel()
                customers = []
                if(total < 25):
                    max = total
                    for x in range(max-1, -1, -1):
                        customer = cm.get_customer_by_id(self.cursor, customers_dict[x])
                        customers += customer
                else:
                    if(total > 999):
                        total = 999
                        limit = 25
                    for x in range(0, 25, 1):
                        customer = cm.get_customer_by_id(self.cursor, customers_dict[x])
                        print(customers_dict[x])
                        customers += customer
                data['total'] = total
                data['mycustomer'] = customers
        return data

    
    def get_searched_mycustomer(self, username_id: str, search: str):
        data = {}
        where = """WHERE is_deleted = 0 """
        where += """AND name LIKE '%{0}%' ORDER BY create_date DESC""".format(search)
        customer_data = self.customer_model.get_all_customer(self.cursor, where=where, limit=100)                            
        data['total'] = customer_data.__len__()
        data['mycustomer'] = customer_data
        return data


    def get_searched_mycustomer_only(self, username_id: str, search: str):
        data = {}
        user = self.cisangkan_user_model.m_get_mycustomer_by_user_id(self.cursor, username_id)
        list_customer = json.loads(user[0]['customer_id'])
        where = """WHERE is_deleted = 0 """
        if list_customer:
            where += "AND `code` IN ('{0}') ".format(
                "', '".join(x for x in list_customer)
            )
        where += """AND name LIKE '%{0}%' ORDER BY create_date DESC""".format(search)
        customer_data = self.customer_model.get_all_customer(self.cursor, where=where, limit=100)                            
        data['total'] = customer_data.__len__()
        data['mycustomer'] = customer_data
        return data        


    def update_customers_sales(self, username: str, customer_id: str):
        user = self.cisangkan_user_model.m_get_mycustomer_by_username(self.cursor, username)
        updated = []
        exist = False
        if (len(user) == 0) or (user == None):
            raise BadRequest("This User not exist", 200, 1)
        else:
            customers_dict = json.loads(user[0]['customer_id'])
        if(customers_dict.__len__() > 0):
            for x in customers_dict:
                if(customer_id == x):
                    exist = True
        result = 0
        if(not exist):
            updated = [customer_id] + customers_dict
            user_data = {
                "username": username,
                "customer_id": updated
            }
            try:
                result = self.cisangkan_user_model.m_update_by_username(self.cursor, user_data)
                mysql.connection.commit()
            except Exception as e:
                raise BadRequest(e, 200, 1)
        return result

    
    def delete_customers_sales(self, username_id:str, customer_id:str):
        customer_data = {
            "code": customer_id,
            "is_deleted": 1,
            "is_delete_approval_by":username_id,
            "is_delete_count":1
        }
        try:
            result = self.cisangkan_customer_model.update_delete_customer(self.cursor, customer_data)
            mysql.connection.commit()
        except Exception as e:
            raise BadRequest(e, 200, 1)

        if(result == 1):
            try:
                query = self.cisangkan_user_model.m_get_mycustomer_by_user_id(self.cursor, username_id)
                customers = json.loads(query[0]['customer_id'])
                if(customers.__len__() > 0):
                    customer = []
                    for x in customers:
                        if(x != customer_id):
                            customer.append(x)
                    user_data = {
                        "id": username_id,
                        "customer_id": customer
                    }
                    query = self.cisangkan_user_model.m_update_by_user_id(self.cursor, user_data)
                    mysql.connection.commit()
            except Exception as e:
                raise BadRequest(e, 200, 1)

        return result


    def create_mycustomer(self, customer_data: 'dict', user_id: 'int'):
        today = datetime.today()
        today = today.strftime("%Y-%m-%d %H:%M:%S")
        try:
            result = self.cisangkan_customer_model.insert_into_db(self.cursor, code=customer_data['code'],
                                                        name=customer_data['name'], email=customer_data['email'],
                                                        phone=customer_data['phone'], address=customer_data['address'],
                                                        lng=customer_data['lng'], lat=customer_data['lat'],
                                                        category=customer_data['category'],
                                                        username=customer_data['username'],
                                                        password=None,
                                                        nfcid=customer_data['nfcid'],
                                                        contacts=customer_data['contacts'],
                                                        business_activity=customer_data['business_activity'],
                                                        is_branch=customer_data['is_branch'],
                                                        parent_code=(customer_data['parent_code'] if (
                                                                'parent_code' in customer_data) else ""),
                                                        create_date=today, update_date=today,
                                                        is_approval=1,
                                                        approval_by=customer_data['approval_by'], create_by=user_id)
            mysql.connection.commit()
            last_insert_id = customer_data['code']
        except Exception as e:
            raise BadRequest(e, 500, 1, data=[])

        return last_insert_id


    def create_summary_plan(self, create_data: 'dict', user_id: 'int'):
        today = datetime.today()
        today = today.strftime("%Y-%m-%d %H:%M:%S")
        category_visit = create_data.get("category_visit", None)
        nc = create_data.get("nc", None)
        create_data["category_visit"] = category_visit
        create_data["nc"] = nc
        try:
            result = self.cisangkan_visit_plan_summary_model.insert_into_db(
                self.cursor, 
                plan_id=create_data["plan_id"], 
                customer_code=create_data["customer_code"],
                notes=create_data["notes"], 
                visit_images=create_data["visit_images"],
                have_competitor=create_data["have_competitor"], 
                competitor_images=create_data["competitor_images"],
                create_date=today,
                update_date=today, 
                create_by=user_id, 
                category_visit=create_data["category_visit"],
                nc=nc
            )
            mysql.connection.commit()
            last_insert_id = self.cursor.lastrowid
        except Exception as e:
            raise BadRequest(e, 500, 1, data=[])

        return last_insert_id

    def create_summary_plan_collector(self, create_data: 'dict', user_id: 'int'):
        today = datetime.today()
        today = today.strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            result = self.cisangkan_visit_plan_summary_model.insert_into_db_collector(
                self.cursor, 
                plan_id=create_data["plan_id"], 
                customer_code=create_data["customer_code"],
                notes=create_data["notes"], 
                visit_images=create_data["visit_images"],
                have_competitor=create_data["have_competitor"], 
                competitor_images=create_data["competitor_images"],
                create_date=today,
                update_date=today, 
                create_by=user_id, 
                collect_method=create_data["collect_method"]
            )
            mysql.connection.commit()
            last_insert_id = self.cursor.lastrowid
        except Exception as e:
            raise BadRequest(e, 500, 1, data=[])

        return last_insert_id        


    def update_summary_plan(self, update_data: 'dict'):
        try:
            result = self.cisangkan_visit_plan_summary_model.update_by_id(self.cursor, update_data)
            mysql.connection.commit()
        except Exception as e:
            raise BadRequest(e, 200, 1)

        return result  
        
    def genSuffixCollectMethod(self, suffix):
        if("Cheque" in suffix):
            suffix = suffix.replace("Cheque", "Cq")            
        if("Giro" in suffix):
            suffix = suffix.replace("Giro", "Go")
        if("Kontra bon" in suffix):
            suffix = suffix.replace("Kontra bon", "Kb")
        if("Transfer" in suffix):
            suffix = suffix.replace("Transfer", "Tf")
        if("Tunai" in suffix):
            suffix = suffix.replace("Tunai", "Tn")
        if("Delivery invoice" in suffix):
            suffix = suffix.replace("Delivery invoice", "Dv")
        if("Tidak tertagih" in suffix):
            suffix = suffix.replace("Tidak tertagih", "Tt")
        if("," in suffix):
            suffix = suffix.replace(",", "_")            
        return suffix

    def saveImageToPath(self, input : 'dict'):            
        suffix = None
        if input['isLogistic']:
            ROOT_IMAGES_PATH = current_app.config["UPLOAD_FOLDER_IMAGES"] + "/delivery_plan_summary/"
        elif input['isCollector']:
            ROOT_IMAGES_PATH = current_app.config["UPLOAD_FOLDER_IMAGES"] + "/collect_plan_summary/"
            cm = input['collect_method']
            suffix = self.genSuffixCollectMethod(cm)
        else:
            ROOT_IMAGES_PATH = current_app.config["UPLOAD_FOLDER_IMAGES"] + "/visit_plan_summary/"

        sales_id = input["username"]
        sales_username = self.getUsernameById(sales_id)
        today = datetime.today()
        today = today.strftime("%Y%m%d")
        cm = CustomerModel()
        customer = cm.get_customer_by_id(self.cursor, input["customer_code"])
                
        if(customer):
            customer_name = customer[0]["name"]
        else:
            customer_name = 'undefined'

        if input["isCollector"]:            
            curr_filename = "{0}_{1}_{2}_{3}_".format(sales_username, today, input["customer_code"], suffix)            
        else:
            curr_filename = "{0}_{1}_{2}_".format(sales_username, today, customer_name)

        if (input["visit_images"] != None):
            current_path = ROOT_IMAGES_PATH + 'visit/'

            if input["isCollector"]:
                exist = self.cisangkan_visit_plan_summary_model.get_visit_plan_summary_by_plan_id_customer_code(self.cursor, input["plan_id"], input["customer_code"])
                if exist :
                    vi = json.loads(exist[0]["visit_images"])
                    for x in vi:
                        fileToRemove = x["image"]
                        if os.path.exists(fileToRemove) :
                            os.remove(fileToRemove)
                else:
                    print("no exist")
            else:
                for x in range(0, 10, 1):
                    fileToRemove = current_path + curr_filename + str(x) + ".jpg"
                    if os.path.exists(fileToRemove) :
                        os.remove(fileToRemove)

            convert_data = []
            i = 0
            for dic in input['visit_images']:
                datas = {}
                for key in dic:
                    if key == 'desc':
                        datas['desc'] = dic[key]
                    if key == 'image':
                        if not os.path.exists(current_path):
                            os.makedirs(current_path)
                        curr_image = base64.b64decode(dic[key])                            
                        pathfile = current_path + curr_filename + str(i) +".jpg"
                        with open(pathfile, 'wb') as f:
                            f.write(curr_image)
                            f.close()
                        datas["image"] = pathfile
                i += 1
                convert_data.append(datas)
            input["visit_images"] = convert_data

        if (input["competitor_images"] != None):
            current_path = ROOT_IMAGES_PATH + 'competitor/'
            for x in range(0, 10, 1):
                fileToRemove = current_path + curr_filename + str(x) + ".jpg"
                print("competitor : " + fileToRemove)
                if os.path.exists(fileToRemove) :
                    os.remove(fileToRemove)
            convert_data = []
            i = 0
            for dic in input['competitor_images']:
                datas = {}
                for key in dic:
                    if key == 'desc':
                        datas['desc'] = dic[key]
                    if key == 'image':
                        if not os.path.exists(current_path):
                            os.makedirs(current_path)
                        curr_image = base64.b64decode(dic[key])                            
                        pathfile = current_path + curr_filename + str(i) +".jpg"
                        with open(pathfile, 'wb') as f:
                            f.write(curr_image)
                            f.close()
                        datas["image"] = pathfile
                i += 1
                convert_data.append(datas)
            input["competitor_images"] = convert_data
        return input


    def loadImageFromPath(self, input : 'dict'):
        if (input["visit_images"] != None):
            revert_data = []
            i = 0
            for dic in input['visit_images']:
                datas = {}
                for key in dic:
                    if key == 'desc':
                        datas['desc'] = dic[key]
                    if key == 'image':
                        if len(dic[key]) < 255:
                            if os.path.exists(dic[key]):                            
                                f = open(dic[key], "r")
                                if f.mode == "r":
                                    with open(dic[key], 'rb') as imgFile:
                                        image = base64.b64encode(imgFile.read())
                                    datas["image"] = str(image.decode('utf-8'))
                        else:
                            datas["image"] = dic[key]
                i += 1
                revert_data.append(datas)
            input["visit_images"] = revert_data

        if (input["competitor_images"] != None):
            revert_data = []
            i = 0
            for dic in input['competitor_images']:
                datas = {}
                for key in dic:
                    if key == 'desc':
                        datas['desc'] = dic[key]
                    if key == 'image':
                        if os.path.exists(dic[key]):                            
                            f = open(dic[key], "r")
                            if f.mode == "r":
                                with open(dic[key], 'rb') as imgFile:
                                    image = base64.b64encode(imgFile.read())
                                datas["image"] = str(image.decode('utf-8'))
                i += 1
                revert_data.append(datas)
            input["competitor_images"] = revert_data
        return input


    def get_last_mycustomer_inserted_today(self, user_id : 'int'):
        try:
            result = self.cisangkan_customer_model.m_get_last_customer_inserted_today(self.cursor, user_id)
            mysql.connection.commit()
        except Exception as e:
            raise BadRequest(e, 200, 1)

        return result


    def get_visit_plan_summary_by_id(self, _id: 'int'):
        result = self.cisangkan_visit_plan_summary_model.get_visit_plan_summary_by_id(self.cursor, _id)
        return result
            

    def saveImageToPath2(self, input : 'dict'):
        ROOT_IMAGES_PATH = current_app.config["UPLOAD_FOLDER_IMAGES"] + "/visit_plan_summary/"
        sales_id = input["username"]
        today = datetime.strptime(str(input["create_date"]), '%Y-%m-%d %H:%M:%S')
        create_date = today.strftime("%Y%m%d")
        cm = CustomerModel()
        customer = cm.get_customer_by_id(self.cursor, input["customer_code"])
        if(customer):
            customer_name = customer[0]["name"]
        else:
            customer_name = 'undefined'
        curr_filename = "{0}_{1}_{2}_".format(sales_id, str(create_date), customer_name)
        if (input["visit_images"] != None):
            current_path = ROOT_IMAGES_PATH + 'visit/'
            for x in range(0, 10, 1):
                fileToRemove = current_path + curr_filename + str(x) + ".jpg"
                if os.path.exists(fileToRemove) :
                    os.remove(fileToRemove)
            convert_data = []
            i = 0
            for dic in input['visit_images']:
                datas = {}
                for key in dic:
                    if key == 'desc':
                        datas['desc'] = dic[key].replace('\n',' ')
                    if key == 'image':
                        if not os.path.exists(current_path):
                            os.makedirs(current_path)
                        curr_image = base64.b64decode(dic[key])                            
                        pathfile = current_path + curr_filename + str(i) +".jpg"
                        with open(pathfile, 'wb') as f:
                            f.write(curr_image)
                            f.close()
                        datas["image"] = pathfile
                i += 1
                convert_data.append(datas)
            input["visit_images"] = convert_data

        if (input["have_competitor"] == 1):
            current_path = ROOT_IMAGES_PATH + 'competitor/'
            for x in range(0, 10, 1):
                fileToRemove = current_path + curr_filename + str(x) + ".jpg"
                print("competitor : " + fileToRemove)
                if os.path.exists(fileToRemove) :
                    os.remove(fileToRemove)
            convert_data = []
            i = 0
            for dic in input['competitor_images']:
                datas = {}
                for key in dic:
                    if key == 'desc':
                        datas['desc'] = dic[key].replace('\n',' ')
                    if key == 'image':                        
                        if not os.path.exists(current_path):
                            os.makedirs(current_path)
                        curr_image = base64.b64decode(dic[key])                            
                        pathfile = current_path + curr_filename + str(i) +".jpg"
                        with open(pathfile, 'wb') as f:
                            f.write(curr_image)
                            f.close()
                        datas["image"] = pathfile
                i += 1
                convert_data.append(datas)
            input["competitor_images"] = convert_data
        return input


    def import_packing_slip_file_csv(self, filename: str, filename_origin: str, table: str, user_id: int):
        df = pd.read_csv(filename, sep=";", skiprows=0)
        headers = df[
            ['INVNO', 'DOCUMENTNUMBER', 'CUSTOMERKEY', 'ITEMKEY', 'ITEMDESCRIPTION', 'QTYSHIPPED', 'SHIPDATE', 'BRANCH CODE', 'DIVISION CODE']
        ]
        data_json = headers.to_json(orient='index', date_format='iso')
        data_json = json.loads(data_json)
        batch_data = []
        today = datetime.today()
        today = today.strftime("%Y-%m-%d %H:%M:%S")
        
        # check if sjc has more than 1 row
        # print("json 1", data_json["1"])
        sjc_idx = []
        batch_data = []
        for i in range (0, data_json.__len__()):
            str_key = str(i)
            value = dict()
            value["code"] = data_json[str_key]["DOCUMENTNUMBER"]
            value["sales_order_code"] = data_json[str_key]["INVNO"]
            delivery_date = dateutil.parser.parse(data_json[str_key]['SHIPDATE']).strftime("%Y-%m-%d %H:%M:%S")
            value["date"] = delivery_date
            value["customer_code"] = data_json[str_key]["CUSTOMERKEY"]
            value["import_date"] = today
            value["update_date"] = today
            value["import_by"] = user_id
            if data_json[str_key]["BRANCH CODE"]:
                try:
                    branch_id = self.branch_model.get_branches_by_code(self.cursor, code=data_json[str_key]["BRANCH CODE"])[0]
                    value['branch_id'] = branch_id['id']
                except:
                    value['branch_id'] = 1
            else:
                value['branch_id'] = 1
            if data_json[str_key]["DIVISION CODE"]:
                try:
                    division_id = self.division_model.get_division_by_code(
                        self.cursor, code=data_json[str_key]["DIVISION CODE"], _id=None
                    )[0]
                    value['division_id'] = division_id['id']
                except:
                    value['division_id'] = 1
            else:
                value['division_id'] = 1
            # product
            obj = dict()
            obj["quantity"] = data_json[str_key]["QTYSHIPPED"]
            obj["product_name"] = data_json[str_key]["ITEMDESCRIPTION"]
            obj["item_number"] = data_json[str_key]["ITEMKEY"]
            obj["brand_code"] = data_json[str_key]["DOCUMENTNUMBER"]
            obj["brand_name"] = ""
            obj["division_code"] = ""
            objs = []
            objs.append(obj)
            value["product"] = objs

            idx_merge = None
            if value["code"] not in sjc_idx:
                sjc_idx.append(value["code"])
                batch_data.append(value)
            else:
                for rec in batch_data:
                    if rec["code"] == value["code"]:
                        rec["product"] += objs       

        insert_ok = 0
        insert_fail = 0
        duplicate = []
        result = {
            "success":1,
            "data": None,
            "duplicates": None
        }
        for rec in batch_data:
            exist = None
            try:
                exist = self.cisangkan_packing_slip_model.get_packing_slip_by_id(self.cursor, rec['code'])
                if(exist):
                    insert_fail += 1
                    duplicate.append(rec)
                else:
                    self.cisangkan_packing_slip_model.import_insert(self.cursor, rec, 'code')
                    mysql.connection.commit()
                    insert_ok += 1
            except Exception as e:
                pass    
        
        if(insert_ok == batch_data.__len__()):
            try:
                self.cisangkan_packing_slip_model.insert_into_tbl_file_upload(self.cursor, filename_origin, today, user_id)
                mysql.connection.commit()
            except Exception as e:
                pass
        else:
            result["success"] = None
            result["duplicates"] = insert_fail
            result["data"] = duplicate
        
        return result

    
    def update_packing_slip_batch(self, batch_data: dict):
        insert_ok = 0
        result = False

        for rec in batch_data:
            exist = None
            try:
                self.cisangkan_packing_slip_model.import_insert(self.cursor, rec, 'code')
                mysql.connection.commit()
                insert_ok += 1
            except Exception as e:
                print("exception", e)
                pass    
        
        if(insert_ok == batch_data.__len__()):
            result = True           
        
        return result


    def update_customer_batch(self, batch_data: dict):
        insert_ok = 0
        result = False

        for rec in batch_data:
            exist = None
            try:
                self.cisangkan_customer_model.insert_update_batch(self.cursor, rec, 'code')
                mysql.connection.commit()
                insert_ok += 1
            except Exception as e:
                print("exception", e)
                pass
        
        if(insert_ok == batch_data.__len__()):
            result = True           
        
        return result


    def sterilize_input(self, input:'dict'):
        if(input['username'] == 'apps'):
            input.pop('username')
        if(input['api_key']):
            input.pop('api_key')
        if(input['username_code']):
            input['username'] = input['username_code']
            input.pop('username_code')
        return input
        

    def sterilize_input_div(self, input:'dict'):   
        try:
            del input['isLogistic']
        except: pass
        try:
            del input['isCollector']
        except: pass
        return input


    def import_customer_file_csv(self, filename: str, filename_origin: str, table: str, user_id: int):
        headers = ['Customer account', 'Name', 'Telephone', 'Contact Name', 'Contact Email', 'Contact Job',
                   'Contact Phone', 'Contact Mobile', 'Contact Notes', 'Address', 'Longitude', 'Latitude',
                   'Parent Customer Account', 'Category']
        
        today = datetime.today()
        today = today.strftime("%Y-%m-%d %H:%M:%S")
        # 
        df = pd.read_csv(filename, sep=";", skiprows=0)
        headers = df[
            ['Customer account', 'Name', 'Telephone', 'Contact Name', 'Contact Email', 'Contact Job',
                        'Contact Phone', 'Contact Mobile', 'Contact Notes', 'Address', 'Longitude', 'Latitude',
                        'Parent Customer Account', 'Category']
        ]
        data_json = headers.to_json(orient='index', date_format='iso')
        data_json = json.loads(data_json)

        batch_data = []
        for i in range(0, data_json.__len__(), 1):
            counter = str(i)
            obj = dict()
            obj['code'] = data_json[counter]['Customer account']
            obj['name'] = data_json[counter]['Name']
            obj['phone'] = data_json[counter]['Telephone']
            obj['address'] = data_json[counter]['Address']
            obj['lat'] = data_json[counter]['Latitude']
            obj['lng'] = data_json[counter]['Longitude']
            obj['parent_code'] = data_json[counter]['Parent Customer Account']
            obj['is_branch'] = (1 if obj['parent_code'] != None else 0)
            obj['category'] = data_json[counter]['Category']        
            
            contact = dict()
            contact['name'] = data_json[counter]['Contact Name']
            contact['email'] = data_json[counter]['Contact Email']
            contact['job'] = data_json[counter]['Contact Job']
            contact['phone'] = data_json[counter]['Contact Phone']
            contact['mobile'] = data_json[counter]['Contact Mobile']

            contact['note'] = (data_json[counter]['Contact Notes'] if data_json[counter]['Contact Notes'] is not None else "" )
            
            contacts = []
            contacts.append(contact)

            obj['contacts'] = contacts

            batch_data.append(obj)


        user_id = current_identity.id

        insert_ok = 0
        insert_fail = 0
        duplicate = []
        result = {
            "success":1,
            "data": None,
            "duplicates": None
        }

        for rec in batch_data:
            exist = None
            try:
                exist = self.customer_model.get_customer_by_code(self.cursor, rec['code'])
                if(exist):
                    insert_fail += 1
                    duplicate.append(rec)
                else:
                    self.customer_model.insert_into_db(self.cursor, code=rec['code'], name=rec['name'], 
                                                        phone=rec['phone'], address=rec['address'],
                                                        lng=rec['lng'], lat=rec['lat'],
                                                        contacts=rec['contacts'], is_branch=rec['is_branch'],
                                                        parent_code=rec['parent_code'], create_date=today, 
                                                        update_date=today, is_approval=user_id,
                                                        approval_by=user_id, create_by=user_id,
                                                        category=rec['category'],
                                                        email = None, username = None, password = None, nfcid = None, business_activity = None
                                                        )
                    mysql.connection.commit()
                    insert_ok += 1
            except Exception as e:
                print("Exception ", e)
                pass    
        
        if(insert_ok == batch_data.__len__()):
            pass
        else:
            result["success"] = None
            result["duplicates"] = insert_fail
            result["data"] = duplicate

        return result
        

    def socketCheckCollector(self, user_id: int):
        select = "u.id, u.username, e.name, e.is_collector_only"
        join = """u INNER JOIN employee e ON employee_id = e.id """
        where="WHERE u.is_deleted = 0 AND u.id = '{0}' ".format(user_id)
        result = None
        try:
            result = self.cisangkan_user_model.m_get_custom_user_properties(self.cursor, select, join, where)[0]
            mysql.connection.commit()
        except Exception as e:
            raise e
        
        return result


    # fixing bug download xls, filter user more than 1
    def getUsernameById(self, user_id: int):
        um = UserModel()
        user_data = um.get_user_by_id(self.cursor, user_id)
        return user_data[0]["username"]
