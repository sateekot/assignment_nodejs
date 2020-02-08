const { Transform } = require('stream');
const Ajv = require('ajv');
const log = require('./logging');
const moment = require('moment');
const fs = require('fs');

const schema = {
  title: 'record',
  type: 'object',
  properties: {
    customerId: {
      type: 'string',
      pattern: '[A-Z]{2}(.*)[0-9]{4}'
    },
    startTime: {
      type: 'string',
      minLength: 19,
      maxLength: 19
    },
    endTime: {
      type: 'string',
      minLength: 19,
      maxLength: 19
    },
    zone: {
      type: 'string',
      pattern: '[ABC]{1}'
    }  
  },
  required: [ 'customerId','startTime','endTime','zone' ]
};

class SchemaValidator extends Transform {

  constructor(options) {
    super(Object.assign({}, { objectMode: true }, options));
    this.validate = new Ajv().compile(schema);
    this.inValidEntries = [];
  }
  _transform(data, encoding, done) {
    const valid = this.validate(data);
    if (valid) {
      this.push(data);
    }
    else {
      //log.error(`Invalid record: ${JSON.stringify(data)}`);
      let record = {
        customerId: data.customerId,
        startTime: data.startTime,
        endTime: data.endTime,
        zone: data.zone
      };
      this.inValidEntries.push(record);
    }
    done();
  }

  _flush(done) {
    const timestamp = moment().format('YYYYMMDD_HHmmss');
    const fileName = `./error_${timestamp}.json`;
    const stream = fs.createWriteStream(fileName);
    stream.write(Buffer.from(JSON.stringify(this.inValidEntries,undefined,2)));
    stream.end()
    log.info(`Created file for invalid entries ${fileName}`);
    done();
  }
}

module.exports = SchemaValidator;

