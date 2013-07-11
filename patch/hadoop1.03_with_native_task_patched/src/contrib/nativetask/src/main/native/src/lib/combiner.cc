#include "combiner.h"
#include "StringUtil.h"

namespace NativeTask {

class KeyGroupIteratorImpl: public KeyGroupIterator {
protected:
  // for KeyGroupIterator
  KeyGroupIterState _keyGroupIterState;
  KVIterator * _iterator;

  string _currentGroupKey;

  Buffer _key;
  Buffer _value;
  bool _first;

public:
  KeyGroupIteratorImpl(KVIterator * iterator) :
    _keyGroupIterState(NEW_KEY),
    _iterator(iterator),
    _first(false){
  }

  bool nextKey() {
    if (_keyGroupIterState == NO_MORE) {
      return false;
    }

    uint32_t temp;
    while (_keyGroupIterState == SAME_KEY ||
        _keyGroupIterState == NEW_KEY_VALUE) {
      nextValue(temp);
    }
    if (_keyGroupIterState ==  NEW_KEY) {
      if (_first == true) {
        _first = false;
        if (!next()) {
          _keyGroupIterState = NO_MORE;
          return false;
        }
      }
      _keyGroupIterState = NEW_KEY_VALUE;
      _currentGroupKey.assign(_key.data(), _key.length());
      return true;
    }
    return false;
  }

  const char * getKey(uint32_t & len) {
    len = (uint32_t)_key.length();
    return _key.data();
  }

  const char * nextValue(uint32_t & len) {
    char * pos;
    switch (_keyGroupIterState) {
    case NEW_KEY: {
      return NULL;
    }
    case SAME_KEY: {
      if (next()) {
        if (_key.length() == _currentGroupKey.length()) {
          if (fmemeq(_key.data(), _currentGroupKey.c_str(), _key.length())) {
            len = _value.length();
            return _value.data();
          }
        }
        _keyGroupIterState = NEW_KEY;
        return NULL;
      }
      _keyGroupIterState = NO_MORE;
      return NULL;
    }
    case NEW_KEY_VALUE: {
      _keyGroupIterState = SAME_KEY;
      len = _value.length();
      return _value.data();
    }
    case NO_MORE:
      return NULL;
    }
    return NULL;
  }

  bool next() {
    bool result = _iterator->next(_key, _value);
    return result;
  }
};

CombineRunner::CombineRunner(Configurable * combiner) :
  _combiner(combiner),
  _keyGroupCount(0),
  _type(UnknownObjectType){
  if (NULL == _combiner) {
    THROW_EXCEPTION_EX(UnsupportException, "Create combiner failed");
  }
  _type = _combiner->type();
}

KeyGroupIterator * CombineRunner::createKeyGroupIterator(KVIterator * iter) {
  return new KeyGroupIteratorImpl(iter);
}

void CombineRunner::combine(CombineContext context, KVIterator * iterator, IFileWriter * writer) {
   switch (_type) {
   case MapperType: {
       Mapper * mapper = (Mapper*)_combiner;
       mapper->setCollector(writer);

       Buffer key;
       Buffer value;
       while(iterator->next(key, value)) {
         mapper->map(key.data(), key.length(), value.data(), value.length());
       }
       mapper->close();
       delete mapper;
     }
     break;
   case ReducerType:
     {
       Reducer * reducer = (Reducer*)_combiner;
       reducer->setCollector(writer);
       KeyGroupIterator * kg = createKeyGroupIterator(iterator);
       while (kg->nextKey()) {
         _keyGroupCount++;
         reducer->reduce(*kg);
       }
       reducer->close();
       delete reducer;
     }
     break;
   default:
     THROW_EXCEPTION(UnsupportException, "Combiner type not support");
   }
 }

} /* namespace NativeTask */
