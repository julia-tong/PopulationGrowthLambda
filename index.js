const aws = require('aws-sdk');
const AthenaExpress = require('athena-express');
const athenaExpressConfig = { aws };
const athenaExpress = new AthenaExpress(athenaExpressConfig);

exports.handler = async (event) => {
    let zip = event.zip;
    let output = {
        Zip: zip,
        CBSA: 'N/A',
        MSA: 'N/A',
        Pop2014: 'N/A',
        Pop2015: 'N/A'
    };

	try {
        let findCbsa = {
            sql: `SELECT cbsa \
                  FROM zip_cbsa \
                  WHERE zip=${zip}`,
            db: 'populationgrowthdb'
        };
		let cbsaResult = await athenaExpress.query(findCbsa);
        let cbsa = parseInt(cbsaResult.Items[0].cbsa);
        console.log(`Found cbsa: ${cbsa}`);
        if (cbsa == 99999) {
            output.CBSA = cbsa;
            return output
        }
        
        let findAlternateCbsa = {
            sql: `SELECT cbsa \
                  FROM cbsa_msa \
                  WHERE mdiv=${cbsa}`,
            db: 'populationgrowthdb'
        };
        let alternateCbsa = await athenaExpress.query(findAlternateCbsa);
        if (alternateCbsa.Items.length != 0) {
            cbsa = parseInt(alternateCbsa.Items[0].cbsa);
            console.log(`Found alternate cbsa: ${cbsa}`);
        }
        output.CBSA = cbsa;

        let msaQuery = {
            sql: `SELECT name, name2, popestimate2014, popestimate2015 \
                  FROM cbsa_msa \
                  WHERE cbsa=${cbsa} AND lsad='Metropolitan Statistical Area'`,
            db: 'populationgrowthdb'
        };
        let msaResult = await athenaExpress.query(msaQuery);
        let msa = msaResult.Items[0];
        console.log(msa);
        output.MSA = msa.name.substring(1,) + ',' + msa.name2.substring(0, msa.name2.length - 1);
        output.Pop2014 = parseInt(msa.popestimate2014);
        output.Pop2015 = parseInt(msa.popestimate2015);
	} catch (error) {
		console.log(error);
    }
    
    return output;
};
