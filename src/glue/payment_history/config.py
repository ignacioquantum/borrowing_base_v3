from string import Template

ENV_VAR: dict = {
    "dev": {
        "endpoint": "quantum-v2-dev-rds-instance-1.cegv0wkgdkwr.us-east-1.rds.amazonaws.com",
        "port": "3306",
        "tables": {
            'invoice': ['id', 'status_id', 'lease_id', 'invoice_number',
                        'invoice_date', 'amount_untaxed', 'amount_tax', 'odoo_invoice_id', 'cashflow_id'],
            'vat': ['NetVAT', 'VATKept', 'idCF', 'idLease'],
            'leaseparameters': ['LeaseName', 'status_detail', 'Product', 'Currency', 'idLeaseParameters'],
            'payment': ['payment_date', 'amount', 'amount_vat', 'invoice_id', 'status_id'],
            'totalleasecf': ['PPMT', 'IPMT', 'PMT', 're_sync_status', 'idLease', 'idTotalLeaseCF',
                             'dDate', 'dMonth', 'is_residualvalue', 'ResidualVal', 'BuyOut', 'is_initial']
        }
    },
    "prod": {
        "endpoint": "prod",
        "port": "3306",
        "tables": ['invoice', 'keymetrics', 'leaseparameters', 'payment', 'totalleasecf']
    },
    "test": {
        "host": "test"
    }
}

QUERY_TEMPLATE: Template = Template("""
select 
	l.LeaseName,
	i.invoice_number,
	l.status_detail,
	l.Product,
	i.invoice_date,
	(i.amount_untaxed + i.amount_tax) as invoice_amount,
	t.dDate,
	t.dMonth,
	PPMT,
	IPMT,
	PMT,
	l.Currency,
	t.is_residualvalue,
	v.NetVAT,
	v.VATKept,
	(t.ResidualVal + t.BuyOut) AS RV,
	-- COUNT(p.amount) as '# payments',
	-- GROUP_CONCAT(p.payment_date) amounts,
	t.is_initial,
	i.odoo_invoice_id,
	t.idTotalLeaseCF,
	max(p.payment_date) as payment_date,
	IF(DATEDIFF(p.payment_date,t.dDate) > 30, CONCAT("PAR ",DATEDIFF(p.payment_date,t.dDate)),DATEDIFF(p.payment_date,t.dDate))  as days_late,
	sum(p.amount) as payment_amount,
	sum(p.amount_vat) as payment_vat,
	sum(p.amount)-sum(p.amount_vat) as net_payment
from
	totalleasecf t
inner join leaseparameters l on
	l.idLeaseParameters = t.idLease
left join vat v on
	v.idCF = t.idTotalLeaseCF
	and v.idLease = t.idLease
left join invoice i on
	t.idTotalLeaseCF = i.cashflow_id
	and t.idLease = i.lease_id
	and i.status_id in (1,3,4) -- agregar en todos los queries
left join payment p on 
	p.invoice_id = i.id
	and p.status_id not in (2)
where
	t.re_sync_status = 'Active' AND
	t.idLease in (440) # 175
group by
	1,
	2,
	3,
	4,
	5,
	6,
	7,
	8,
	9,
	10,
	11,
	12,
	13,
	14,
	15
order by
	1,7;
""")


