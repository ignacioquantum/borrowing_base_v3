from string import Template

ENV_VAR: dict = {
    "dev": {
        "ssh_host": 'quantum-v2-devprod-brdg-lb-ba4d9f717831e892.elb.us-east-1.amazonaws.com',
        "ssh_port": '22',
        "ssh_user": 'ubuntu',
        "ssh_password": 'heJQAshymxMMhnKW6vth3LlEWbsc',
        "remote_host": 'quantum-v2-dev-rds.cluster-cegv0wkgdkwr.us-east-1.rds.amazonaws.com',
        "db_port": 3306,
        "conection_properties": {
                "user": "idiaz",
                "password": "FI1cYZAN2ICKr7Ads1RaNGgL8lnX",
                "driver": "com.mysql.cj.jdbc.Driver",
                "url": "jdbc:mysql://localhost:",
                "db": "quantum"
            }
    },
    "prod": {
        "ssh_host": 'quantum-v2-devprod-brdg-lb-ba4d9f717831e892.elb.us-east-1.amazonaws.com',
        "ssh_port": '22',
        "ssh_user": 'ubuntu',
        "ssh_password": 'heJQAshymxMMhnKW6vth3LlEWbsc',
        "remote_host": 'quantum-v2-dev-rds.cluster-cegv0wkgdkwr.us-east-1.rds.amazonaws.com',
        "db_port": 3306,
    },
    "test": {
        "ssh_host": 'quantum-v2-devprod-brdg-lb-ba4d9f717831e892.elb.us-east-1.amazonaws.com',
        "ssh_port": '22',
        "ssh_user": 'ubuntu',
        "ssh_password": 'heJQAshymxMMhnKW6vth3LlEWbsc',
        "remote_host": 'quantum-v2-dev-rds.cluster-cegv0wkgdkwr.us-east-1.rds.amazonaws.com',
        "db_port": 3306,
    }
}

BORROWING_TABLES = {
    "leaseparameters": {
        'columns': ["idLeaseParameters","status_detail","LeaseName", "funder", "Product",
                        "status", "DRAW", "Currency", "FX", "Industry","EquipmentType",
                        "Coupon", "FirstPaymentDate", "Duration", "ClosingDate"],
        'alias': 'l'
    },
    "keymetrics": {
        'columns': ["idLease", "LTVFacilityDE", "IRR", "deleted_at"],
        'alias': 'k'
    },
    "totalleasecf": {
        'columns': ["idTotalLeaseCF", "idLease", "dMonth", "IPMT", "PPMT", "PMT", "ResidualVal",
                     "BuyOut", "is_residualvalue", "re_sync_status", "is_initial", "dDate"],
        'alias': 't'
    },
    "vat": {
        'columns': ["idCF", "idLease", "VATKept"],
        'alias': 'v'
    },
    "invoice": {
        'columns': ["id","cashflow_id", "lease_id", "status_id", "is_initial"],
        'alias': 'i'
    },
    "payment": {
        'columns': ["invoice_id", "status_id", "payment_date", "amount", "amount_vat"],
        'alias': 'p'
    }
    ,
}

