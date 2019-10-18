const csvtojson = require('csvtojson');
const jsonQuery = require('json-query');
const request = require('request');

exports.handler = async (event) => {
    try {
        let zip = event.zip;
        let output = {
            Zip: zip,
            CBSA: null,
            MSA: null,
            Pop2014: null,
            Pop2015: null
        };

        let getCbsa = csvtojson().fromStream(request.get('https://population-growth-csv.s3-us-west-2.amazonaws.com/zip_to_cbsa.csv'));
        let getMsa = csvtojson().fromStream(request.get('https://population-growth-csv.s3-us-west-2.amazonaws.com/cbsa_to_msa.csv'));
        let [cbsaArray, msaArray] = await Promise.all([getCbsa, getMsa]);

        let cbsaJson = {data: cbsaArray};
        let cbsa = parseInt(jsonQuery(`data[ZIP=${zip}].CBSA`, {
            data: cbsaJson
        }).value);

        if (cbsa == null) {
            return output;
        } else {
            output.CBSA = cbsa;
            if (cbsa == 99999) {
                return output;
            }
        }

        let msaJson = {data: msaArray};
        let row;
        let alternateCbsa = jsonQuery(`data[MDIV=${cbsa}]`, {
            data: msaJson
        });
        if (alternateCbsa.value != null) {
            row = jsonQuery(`data[CBSA=${alternateCbsa.value.CBSA} & LSAD=Metropolitan Statistical Area]`, {
              data: msaJson
            }).value;
        } else {
            row = jsonQuery(`data[CBSA=${cbsa} & LSAD=Metropolitan Statistical Area]`, {
                data: msaJson
            }).value;
        }

        if (row != null) {
            output.CBSA = row.CBSA;
            output.MSA = row.NAME;
            output.Pop2014 = row.POPESTIMATE2014;
            output.Pop2015 = row.POPESTIMATE2015;
        }
        return output;
    } catch (error) {
        throw Error(error);
    }
};
