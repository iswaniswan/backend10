import re
import json
import time
import pandas as pd
import dateutil.parser
import numpy
import os
import base64

from datetime import datetime
from dateutil.relativedelta import relativedelta
from flask import current_app, render_template

from rest.exceptions import BadRequest, RestException
from rest.helpers import mysql, date_range
from rest.helpers.validator import safe_format, safe_invalid_format_message
from rest.models import CustomerModel, UserModel, CisangkanCustomerModel, CisangkanDeliveryPlanModel, CisangkanUserModel, CisangkanVisitPlanSummaryModel

# from rest.models import ContactModel

__author__ = 'iswan'

# made as requested for custom purpose
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

                    # for x in range(total-1, total-26, -1):
                    for x in range(0, 25, 1):
                        customer = cm.get_customer_by_id(self.cursor, customers_dict[x])
                        print(customers_dict[x])
                        customers += customer

                
                data['total'] = total

                # for x in range(max):
                #     customer = cm.get_customer_by_id(self.cursor, customers_dict[x])
                #     customers += customer

                    # customers.append(customers_dict[x])


                # init = []
                # for x in customers:
                #     init.append(x['code'])
                # data['init'] = init

                data['mycustomer'] = customers

        return data

    
    def get_searched_mycustomer(self, username_id: str, search: str):
        data = {}

        # test code
        # where = """WHERE is_deleted = 0 AND create_by = '{0}' """.format(username_id)
        where = """WHERE is_deleted = 0 """
        where += """AND name LIKE '%{0}%' ORDER BY create_date DESC""".format(search)
        customer_data = self.customer_model.get_all_customer(self.cursor, where=where, limit=100)                            

        # test code end
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

        # test code end
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
            # check if data exist
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
            # update to database
            try:
                result = self.cisangkan_user_model.m_update_by_username(self.cursor, user_data)
                mysql.connection.commit()
            except Exception as e:
                raise BadRequest(e, 200, 1)

        return result

    
    def delete_customers_sales(self, username_id:str, customer_id:str):
        # set is deleted = 1 in table customer
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

        # remove customer_id from table user

        if(result == 1):

            try:
                query = self.cisangkan_user_model.m_get_mycustomer_by_user_id(self.cursor, username_id)
                customers = json.loads(query[0]['customer_id'])

                if(customers.__len__() > 0):
                    customer = []
                    for x in customers:
                        if(x != customer_id):
                            customer.append(x)
                    # customer = customers.remove(customer_id)

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

    def sterilize_input(self, input:'dict'):

        if(input['username'] == 'apps'):
            input.pop('username')
        if(input['api_key']):
            input.pop('api_key')
        if(input['username_code']):
            input['username'] = input['username_code']
            input.pop('username_code')
        
        return input


    def create_summary_plan(self, create_data: 'dict', user_id: 'int'):

        today = datetime.today()
        today = today.strftime("%Y-%m-%d %H:%M:%S")
        
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
                category_visit=create_data["category_visit"]
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

    def saveImageToPath(self, input : 'dict'):
        # convert image to path
        ROOT_IMAGES_PATH = current_app.config["UPLOAD_FOLDER_IMAGES"] + "/visit_plan_summary/"
        # current_path = VISIT_IMAGES_PATH + str(input["plan_id"])  + '/' + input["customer_code"] + '/' 

        # formating structure :
        # visit -> id.sales_ymd_customerName_1, 2, 3..
        # competitor -> id.sales_ymd_customerName_1, 2, 3..
        sales_id = input["username"] 

        # prepare format
        today = datetime.today()
        today = today.strftime("%Y%m%d")

        # prefix_filename
        cm = CustomerModel()
        customer = cm.get_customer_by_id(self.cursor, input["customer_code"])
        customer_name = customer[0]["name"]

        curr_filename = "{0}_{1}_{2}_".format(sales_id, today, customer_name)

        # visit images
        if (input["visit_images"] != None):
            current_path = ROOT_IMAGES_PATH + 'visit/'
            # clear image first if exist
            # try to check 10 image file within prefix file nama
            for x in range(0, 9, 1):
                fileToRemove = current_path + curr_filename + str(x) + ".jpg"
                # print("visit : " + fileToRemove)
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

                        # 
                        # imgdata = base64.b64decode(imgstring)
                        # filename = 'some_image.jpg'  # I assume you have a way of picking unique filenames
                        # with open(filename, 'wb') as f:
                        #     f.write(imgdata)

                        curr_image = base64.b64decode(dic[key])                            
                        pathfile = current_path + curr_filename + str(i) +".jpg"

                        with open(pathfile, 'wb') as f:
                            f.write(curr_image)
                            f.close()
                        # 
                        datas["image"] = pathfile
                i += 1
                convert_data.append(datas)
            input["visit_images"] = convert_data

        # competitor images
        if (input["competitor_images"] != None):
            current_path = ROOT_IMAGES_PATH + 'competitor/'
            # clear image first if exist
            # try to check 10 image file within prefix file nama
            for x in range(0, 9, 1):
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
                        
                        # pathfile = current_path + "competitor_img64_" + str(i) +".txt"
                        # data_to_write = dic[key]
                        # f = open(pathfile, "w")
                        # f.write( str(data_to_write))
                        # f.close()
                        # datas["image"] = pathfile

                        curr_image = base64.b64decode(dic[key])                            
                        pathfile = current_path + curr_filename + str(i) +".jpg"

                        with open(pathfile, 'wb') as f:
                            f.write(curr_image)
                            f.close()
                        # 
                        datas["image"] = pathfile

                i += 1
                convert_data.append(datas)
            input["competitor_images"] = convert_data
        return input

    def loadImageFromPath(self, input : 'dict'):
        # load image from path
        # ROOT_IMAGES_PATH = current_app.config["UPLOAD_FOLDER_IMAGES"] + "/visit_plan_summary/"
        # current_path = VISIT_IMAGES_PATH + str(input["plan_id"])  + '/' + input["customer_code"] + '/' 

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
                                    # datas["image"] = f.read()

                                    #

                                    # remobe 'b string at first
                                    # encoded_string= base64.b64encode(img_file.read())
                                    # print(encoded_string.decode('utf-8'))

                                    with open(dic[key], 'rb') as imgFile:
                                        image = base64.b64encode(imgFile.read())
                                    datas["image"] = str(image.decode('utf-8'))
                                    #
                        else:
                            datas["image"] = dic[key]

                i += 1
                revert_data.append(datas)

            input["visit_images"] = revert_data

        # competitor images
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
                                # datas["image"] = f.read()

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
