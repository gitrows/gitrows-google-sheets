const {google} = require('googleapis')
const sheets = google.sheets('v4')
const response=require('@gitrows/lambda-response')
const Num = require("@gitrows/gitrows-utils/lib/number.js")
const Data = require("@gitrows/gitrows-utils/lib/data.js")

class GoogleSheetsConnector {

	constructor(options){
		/*
		*		require('dotenv').config()
		* 	new gs({CLIENT_ID:process.env.CLIENT_ID,CLIENT_SECRET:process.env.CLIENT_SECRET})
		*/
		this.options(options);
	}

	options(obj){
		let self=this;
		const allowed=['owner','sheet','credentials','CLIENT_ID','CLIENT_SECRET'];
		if (typeof obj=='undefined'){
			let data={};
			allowed.forEach((item, i) => {
				data[item]=this[item];
			});
			return data;
		}
		for (let key in obj) {
			if (allowed.includes(key)&&typeof obj[key]!=='undefined') this[key]=obj[key];
		}
		return self;
	}

	async authorize(token){
		if (typeof token=='undefined') token=this.credentials;
		const oauth2Client = new google.auth.OAuth2(
			this.CLIENT_ID,
			this.CLIENT_SECRET
		);
		if (typeof token==='string')
			token={refresh_token: token};
		oauth2Client.setCredentials(token);
		const tokens = (await oauth2Client.refreshAccessToken()).credentials;
		oauth2Client.setCredentials({
			access_token: tokens.access_token
		});
		this.credentials=tokens;
		this.client=oauth2Client;
		return tokens;
	}

	async create(title, data, order){
		if (!this.client) return Promise.reject(new Error('Unauthorized, please use auth(token) method'));
		let request,response;
		request = {
			auth: this.client,
			resource: {
				properties: {
					title: title||'GitRows Data Connector'
				}
			}
		};
		response=await sheets.spreadsheets.create(request).catch(e=>console.log(e));
		this.sheet=response.data.spreadsheetId;
		if(data){
			response=await this.append(data,order,true);
		}
		return response;
	}

	async append(data,order,columns=false){
		if (!this.sheet) return Promise.reject(new Error('No Sheet ID specified'));
		let request,response,metadata;
		data=_prepareData(data,order);
		if (columns&&data.columns){
			data.values.unshift(data.columns);
			data.keys.unshift('head');
		}
		request = {
			spreadsheetId: this.sheet,
			range: 'Sheet1',
			valueInputOption: 'RAW',
			insertDataOption: 'INSERT_ROWS',
			resource: {
				values: data.values,
			},
			auth: this.client
		};
		response=await sheets.spreadsheets.values.append(request).catch(e=>console.log(e));
		metadata= await this.addMetadataKeys(response.data.updates.updatedRange,data.keys);
		return metadata;
	}

	async addMetadataKeys(range,keys){
		if (!this.sheet) return Promise.reject(new Error('No Sheet ID specified'));
		let request,start,end,data=[];
		if (Num.isNumber(range))
			([start,end]=[range,range+1]);
		else
			({start,end}=rangeToRowIndex(range));
		keys.forEach((item, i) => {
			data.push(createDeveloperMetadataRequest(start+i,item));
		});
		request = {
			spreadsheetId: this.sheet,
			resource: {
				requests: data,
			},
			auth: this.client
		};
		return sheets.spreadsheets.batchUpdate(request).catch(e=>console.log(e));
	}

	async update(data,order){
		if (!this.sheet) return Promise.reject(new Error('No Sheet ID specified'));
		data=_prepareData(data,order);
		let request,batch=[];
		data.values.forEach((item, i) => {
			batch.push(createUpdateRequest(data.keys[i],[item]))
		});
		request={
			spreadsheetId:this.sheet,
			resource: {
			  "data": batch,
			  "valueInputOption": "RAW"
			},
			auth: this.client
		}
		return sheets.spreadsheets.values.batchUpdateByDataFilter(request);
	}
}

const _prepareData=(data,order)=>{
	if (typeof data=='undefined') return data;
	let result={keys:[],values:[],columns:[]};
	if (!Array.isArray(data)) data=[data];
	result.columns=order||Data.columns(data);
	data.forEach((item, i) => {
		if (item.id)
			result.keys[i]=Num.isNumber(item.id)?Num.baseEncode(item.id):item.id;
		else
			result.keys[i]=null;
		result.values[i]=result.columns.map(x=>typeof item[x]=='object'?JSON.stringify(item[x]):item[x]);
	});
	return result;
}

const rangeToRowIndex=(range)=>{
	range=range.split('!').pop();
	let[start,end]=range.replace(/[^\d:]/g,'').split(':');
	const result={start:start?Number(start):null,end:end?Number(end):null};
	return result;
}

const createDeveloperMetadataRequest=(index,key)=>{
	let request={
		"createDeveloperMetadata":
		{
			"developerMetadata": {
				"metadataKey": key,
				"location": {
				 "dimensionRange":{
					 "dimension":"ROWS",
					 "startIndex":index-1,
					 "endIndex":index,
				 }
				},
				"visibility": "DOCUMENT"
			}
		}
	}
	return request;
}

const createUpdateRequest=(key,data)=>{
	let request={
		"dataFilter": {
			"developerMetadataLookup": {
				"metadataKey": key
			}
		},
		"majorDimension": "ROWS",
		"values": data
	}
	return request;
}

module.exports = GoogleSheetsConnector;
