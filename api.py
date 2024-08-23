from flask import Flask, jsonify, request
from flask_restful import Resource, Api
import psycopg2

class Quote(Resource):
    def make_json_answer_(self, answer, cur):
        id_vacancy = answer[0]
        state = answer[2]
        role = answer[3]
        address = answer[4]
        experience = answer[5]
        employment = answer[6]
        salary = answer[7]
        id_empl = answer[8]
        cur.execute("SELECT name FROM employers WHERE id_employer = " + str(id_empl))
        name_employer = str(cur.fetchall()[0])[1:-2]
        url = answer[9]
        quote = {
            "id": id_vacancy,
            "state": state,
            "role": role,
            "address": address,
            "aeperience": experience,
            "employment": employment,
            "salary": salary,
            "employer": {"id": id_empl, "name": name_employer},
            "url": url
            }
        return quote

    def get(self):
        conn = psycopg2.connect(database="practice5", user="postgres", password="rere2004", host="localhost", port=5432)
        cur = conn.cursor()
        cur_empl = conn.cursor()

        empl_condition = ""
        area_condition = ""
        role_condition = ""
        state_condition = ""

        employment =  request.args.get('employment')
        if employment != None:
            if employment == "full":
                employment = "'Полная занятость'"
            elif employment == "part":
                employment = "'Частичная занятость'"
            elif employment == "project":
                employment = "'Проектная работа'"
            elif employment == "probation":
                employment = "'Стажировка'"
            elif employment == "volunteer":
                employment = "'Волонтерство'"
            empl_condition = "employment = " + employment

        role = request.args.get('text')
        if role != None:
            role_condition = "role LIKE '%" + role.lower() +"%'"

        state = request.args.get('open')
        if state != None:
            if state == 'true':
                state = "'open'"
            if state == 'false':
                state = "'close"
            state_condition = "state = " + state

        if request.args.get('area') != None:
            area_condition = "address->>'city' = '" + request.args.get('area') + "'"
        
        execution_str = "SELECT * FROM vacancies "
        if (empl_condition != "" or area_condition != "" or role_condition != "" or state_condition != ""):
            execution_str += "WHERE "

            if empl_condition != "": execution_str += empl_condition

            if area_condition != "":
                if "=" in execution_str: execution_str += " and " + area_condition
                else: execution_str += area_condition
            
            if role_condition != "": 
                if "=" in execution_str: execution_str += " and " + role_condition
                else: execution_str += role_condition
            
            if state_condition != "": 
                if "=" in execution_str: execution_str += " and " + state_condition
                else: execution_str += state_condition

        cur.execute(execution_str)
        cur_empl
        d = [self.make_json_answer_(a, cur_empl) for a in cur]
        return d, 200

app = Flask(__name__)
api = Api(app)
api.add_resource(Quote, '/api/vacancy/')
app.run(debug=True)
