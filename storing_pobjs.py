import pickle
class Company:
    def __init__(self, name, value):
        self.name = name
        self.value = value

with open('company_data.pkl', 'wb') as outp:
    company1 = Company('banana', 40)
    pickle.dump(company1, outp, pickle.HIGHEST_PROTOCOL)


def pickle_loader(filename):
    """ Deserialize a file of pickled objects. """
    with open(filename, "rb") as f:
        while True:
            try:
                yield pickle.load(f)
            except EOFError:
                break

print('Companies in pickle file:')
for company in pickle_loader('company_data.pkl'):
    print('  name: {}, value: {}'.format(company.name, company.value))