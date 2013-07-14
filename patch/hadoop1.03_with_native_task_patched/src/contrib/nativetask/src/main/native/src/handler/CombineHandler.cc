#include "CombineHandler.h"

namespace NativeTask {

const char * JAVA_COMBINE_COMMAND = "combine";
const char * REFILL = "refill";
const int LENGTH_OF_REFILL = 6;

CombineHandler::CombineHandler() :
      _combineContext(NULL),
       _kvIterator(NULL),
      _writer(NULL),
      _kvCached(false){

  LOG("combiner class is created, address %lld", this);
}

CombineHandler::~CombineHandler() {
  // TODO Auto-generated destructor stub
}

void CombineHandler::configure(Config & config) {

  _config = &config;
  MapOutputSpec::getSpecFromConfig(config, _mapOutputSpec);
  _kType = _mapOutputSpec.keyType;
  _vType = _mapOutputSpec.valueType;
}

void CombineHandler::delegateToJavaCombiner() {
  string result = this->sendCommand(JAVA_COMBINE_COMMAND);
}

uint32_t CombineHandler::refill(int serializationType) {
  _ob.position = 0;
  while(_kvCached || _kvIterator->next(_key, _value)) {
  _kvCached = false;
  const char * key = _key.data();
  uint32_t keyLen = _key.length();
  const char * value = _value.data();
  uint32_t valueLen = _value.length();

  char vIntBuffer[8];

    //flush output to make sure we have enough room to hold the key and value
    if (_ob.position + keyLen + valueLen + 2 * sizeof(uint32_t) + 32 > _ob.capacity) {
      _kvCached = true;
      return _ob.position;
    }

    if (serializationType == 0) {
      switch (_kType) {
        case TextType:

          uint32_t vlen = 0;
          WritableUtils::WriteVInt(keyLen, vIntBuffer, vlen);
          putInt(bswap(keyLen + vlen));
          put(vIntBuffer, vlen);
          put((char *)key, keyLen);
          break;
        case BytesType:
          putInt(bswap(keyLen + 4));
          putInt(bswap(keyLen));
          put((char *)key, keyLen);
          break;
        default:
          putInt(bswap(keyLen));
          put((char *)key, keyLen);
          break;
      }

      switch (_vType) {
        case TextType:

          uint32_t vlen = 0;
          WritableUtils::WriteVInt(valueLen, vIntBuffer, vlen);
          putInt(bswap(valueLen + vlen));
          put(vIntBuffer, vlen);
          put((char *)value, valueLen);
          break;
        case BytesType:
          putInt(bswap(valueLen + 4));
          putInt(bswap(valueLen));
          put((char *)value, valueLen);
          break;
        default:
          putInt(bswap(valueLen));
          put((char *)value, valueLen);
          break;
      }
    }
    else {
      putInt(bswap(keyLen));
      put((char *)key, keyLen);
      putInt(bswap(valueLen));
      put((char *)value, valueLen);
    }
  }
  return _ob.position;
}

void CombineHandler::handleInput(char * buff, uint32_t length) {
  while (length>0) {
     if (unlikely(length<2*sizeof(uint32_t))) {
       THROW_EXCEPTION(IOException, "k/v meta information incomplete");
     }

     //convert to little endium
     char * pos = buff;
     uint32_t keyLength = bswap(*((uint32_t*)pos));

     pos += keyLength + sizeof(uint32_t);
     uint32_t valueLength = bswap(*((uint32_t*)pos));

     pos += valueLength + sizeof(uint32_t);

     uint32_t kvlength = pos - buff;
     if (unlikely(kvlength > length)) {
       //key and value must be able to flushed together.
     THROW_EXCEPTION(IOException, "k/v data incomplete");
     }

     _writer->write(buff + 4, keyLength, buff + keyLength + 8, valueLength);
     buff += kvlength;
     length -= kvlength;
   }

}

string  toString(uint32_t length) {
  string result;
  result.reserve(4);
  result.assign((char *)(&length), 4);
  return result;
}


std::string CombineHandler::command(const std::string & cmd) {

  if ( 0 == memcmp(cmd.c_str(), REFILL, LENGTH_OF_REFILL)) {
    uint32_t * serialzationType = (uint32_t *)(cmd.c_str() +  LENGTH_OF_REFILL);
    uint32_t type = bswap(*serialzationType);
    return toString(bswap(refill(type)));
  }

  THROW_EXCEPTION(UnsupportException, "Command not supported by RReducerHandler");
}

void CombineHandler::combine(CombineContext type, KVIterator * kvIterator, IFileWriter * writer) {

  this->_combineContext = &type;
  this->_kvIterator = kvIterator;
  this->_writer = writer;
  delegateToJavaCombiner();
  return;
}

void CombineHandler::finish() {
}

} /* namespace NativeTask */
