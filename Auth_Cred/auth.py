from simple_salesforce import Salesforce

def connect_salesforce(config):
    """Connects to Salesforce and returns the Salesforce object."""
    return Salesforce(
        username=config["username"],
        password=config["password"],
        security_token=config["security_token"],
        domain=config["domain"],
        version='61.0'
    )
