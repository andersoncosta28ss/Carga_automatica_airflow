a = {'error': 500, 'message': 'this credential has an invalid status', 'details': [{'httpException': 'Internal Server Error'}]}
print(a.__contains__('error'))