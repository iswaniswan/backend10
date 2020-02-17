import json

from rest.helpers import mysql
from .model import Model


__author__ = 'iswan'

# made as requested for custom purpose
# dibuat sesuai keperluan

class CisangkanCustomerModel(Model):
    def __init__(self):
        Model.__init__(self)
        self.table = 'customer'

    def update_coordinate_by_id(self, cursor, customer_data):
        try:
            return self.update_key(cursor, customer_data, 'code', 'is_deleted', 0)
        except Exception as e:
            raise e

    def update_delete_customer(self, cursor, customer_data):
        try:
            return self.update(cursor, customer_data, 'code')
        except Exception as e:
            raise e 

    def insert_into_db(self, cursor, code, name, email, phone, address, lng, lat, category, username, password, nfcid, contacts,
                       business_activity, is_branch, parent_code, create_date, update_date, is_approval, approval_by,
                       create_by):
        try:
            value = {
                "code": code, "name": name, "email": email, "phone": phone, "address": address, "lng": lng, "lat": lat, "category": category,
                "username": username, "password": password, "nfcid": nfcid, "contacts": contacts,
                "business_activity": business_activity, "is_branch": is_branch, "parent_code": parent_code,
                "create_date": create_date,
                "update_date": update_date, "is_approval": is_approval,
                "approval_by": approval_by, "create_by": create_by
            }
            return self.insert_ignore(cursor, value)
        except Exception as e:
            raise e        


    def m_get_last_customer_inserted_today(self, cursor, user_id):
        # mysql syntax
        # select count(code) from cisangkan.customer where create_by = 115 and DATE(create_date) = CURDATE();
        select = "COUNT(code) as last"
        where = "WHERE create_by = {} AND DATE(create_date) = CURDATE()".format(user_id)
        try:
            return self.get(cursor, fields=select, where=where)
        except Exception as e:
            raise e
    
class CisangkanUserModel(Model):
    def __init__(self):
        Model.__init__(self)
        self.table = 'users'

    def m_get_mycustomer_by_username(self, cursor, username, select='customer_id'):
        try:
            return self.get(cursor, fields=select, where="WHERE username = '{}' AND is_deleted = 0".format(username))
        except Exception as e:
            raise e

    def m_get_mycustomer_by_user_id(self, cursor, id, select='customer_id'):
        try:
            return self.get(cursor, fields=select, where="WHERE id = '{}'".format(id))
        except Exception as e:
            raise e

    def m_update_by_username(self, cursor, user_data):
        try:
            return self.update(cursor, user_data, 'username')
        except Exception as e:
            raise e

    def m_update_by_user_id(self, cursor, user_data):
        try:
            return self.update(cursor, user_data, 'id')
        except Exception as e:
            raise e   

class CisangkanDeliveryPlanModel(Model):
    def __init__(self):
        Model.__init__(self)
        self.table = 'delivery_plan'

    def update_coordinate_delivery_plan(self, cursor, data):
        try:
            str_coordinate = ''
            updated_rows = (
                session.query(self)
                .filter(delivery_plan.id == id)
                .update({delivery_plan.destination_order: func.replace(delivery_plan.destination_order, '"lat": 0, "lng": 0,', str_coordinate)},
                        synchronize_session=False)
                )

            return ("Updated {} rows".format(updated_rows))

        except Exception as e:
            raise e

class CisangkanVisitPlanSummaryModel(Model):
    def __init__(self):
        Model.__init__(self)
        self.table = 'visit_plan_summary'

    def insert_into_db(
            self, cursor, plan_id, customer_code, notes, visit_images, have_competitor, competitor_images,
            create_date, update_date, create_by, category_visit
    ):
        try:
            value = {
                "plan_id": plan_id, "customer_code": customer_code, "notes": notes, "visit_images": visit_images,
                "have_competitor": have_competitor, "competitor_images": competitor_images,
                "create_date": create_date, "update_date": update_date, "create_by": create_by, "category_visit":category_visit
            }
            return self.insert_ignore(cursor, value)
        except Exception as e:
            raise e


    def update_by_id(self, cursor, update_data):
        try:
            return self.update(cursor, update_data, 'id')
        except Exception as e:
            raise e


