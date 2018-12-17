// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/descriptors.h"

#include <boost/algorithm/string/join.hpp>
#include <ios>
#include <sstream>

#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/IR/DataLayout.h>

#include "codegen/llvm_codegen.h"
#include "common/object_pool.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/descriptors.pb.h"
#include "gen_cpp/PlanNodes_types.h"
#include "exprs/expr.h"

namespace doris {
using boost::algorithm::join;

const int RowDescriptor::INVALID_IDX = -1;
std::string NullIndicatorOffset::debug_string() const {
    std::stringstream out;
    out << "(offset=" << byte_offset
        << " mask=" << std::hex << static_cast<int>(bit_mask) << std::dec << ")";
    return out.str();
}

std::ostream& operator<<(std::ostream& os, const NullIndicatorOffset& null_indicator) {
    os << null_indicator.debug_string();
    return os;
}

SlotDescriptor::SlotDescriptor(const TSlotDescriptor& tdesc)
    : _id(tdesc.id),
      _type(TypeDescriptor::from_thrift(tdesc.slotType)),
      _parent(tdesc.parent),
      _col_pos(tdesc.columnPos),
      _tuple_offset(tdesc.byteOffset),
      _null_indicator_offset(tdesc.nullIndicatorByte, tdesc.nullIndicatorBit),
      _col_name(tdesc.colName),
      _slot_idx(tdesc.slotIdx),
      _slot_size(_type.get_slot_size()),
      _field_idx(-1),
      _is_materialized(tdesc.isMaterialized),
      _is_null_fn(NULL),
      _set_not_null_fn(NULL),
      _set_null_fn(NULL) {
}

SlotDescriptor::SlotDescriptor(const PSlotDescriptor& pdesc)
        : _id(pdesc.id()),
        _type(TypeDescriptor::from_protobuf(pdesc.slot_type())),
        _parent(pdesc.parent()),
        _col_pos(pdesc.column_pos()),
        _tuple_offset(pdesc.byte_offset()),
        _null_indicator_offset(pdesc.null_indicator_byte(), pdesc.null_indicator_bit()),
        _col_name(pdesc.col_name()),
        _slot_idx(pdesc.slot_idx()),
        _slot_size(_type.get_slot_size()),
        _field_idx(-1),
        _is_materialized(pdesc.is_materialized()),
        _is_null_fn(NULL),
        _set_not_null_fn(NULL),
        _set_null_fn(NULL) {
}

void SlotDescriptor::to_protobuf(PSlotDescriptor* pslot) const {
    pslot->set_id(_id);
    pslot->set_parent(_parent);
    _type.to_protobuf(pslot->mutable_slot_type());
    pslot->set_column_pos(_col_pos);
    pslot->set_byte_offset(_tuple_offset);
    pslot->set_null_indicator_byte(_null_indicator_offset.byte_offset);
    pslot->set_null_indicator_bit(_null_indicator_offset.bit_offset);
    pslot->set_col_name(_col_name);
    pslot->set_slot_idx(_slot_idx);
    pslot->set_is_materialized(_is_materialized);
}

std::string SlotDescriptor::debug_string() const {
    std::stringstream out;
    out << "Slot(id=" << _id << " type=" << _type
        << " col=" << _col_pos << " offset=" << _tuple_offset
        << " null=" << _null_indicator_offset.debug_string() << ")";
    return out.str();
}

TableDescriptor::TableDescriptor(const TTableDescriptor& tdesc)
    : _name(tdesc.tableName),
      _database(tdesc.dbName),
      _id(tdesc.id),
      _num_cols(tdesc.numCols),
      _num_clustering_cols(tdesc.numClusteringCols) {
}

std::string TableDescriptor::debug_string() const {
    std::stringstream out;
    out << "#cols=" << _num_cols << " #clustering_cols=" << _num_clustering_cols;
    return out.str();
}

OlapTableDescriptor::OlapTableDescriptor(const TTableDescriptor& tdesc)
    : TableDescriptor(tdesc) {
}

std::string OlapTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "OlapTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

SchemaTableDescriptor::SchemaTableDescriptor(const TTableDescriptor& tdesc)
    : TableDescriptor(tdesc),
      _schema_table_type(tdesc.schemaTable.tableType) {
}
SchemaTableDescriptor::~SchemaTableDescriptor() {
}

std::string SchemaTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "SchemaTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

BrokerTableDescriptor::BrokerTableDescriptor(const TTableDescriptor& tdesc)
    : TableDescriptor(tdesc) {
}

BrokerTableDescriptor::~BrokerTableDescriptor() {
}

std::string BrokerTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "BrokerTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

KuduTableDescriptor::KuduTableDescriptor(const TTableDescriptor& tdesc)
  : TableDescriptor(tdesc),
    table_name_(tdesc.kuduTable.table_name),
    key_columns_(tdesc.kuduTable.key_columns),
    master_addresses_(tdesc.kuduTable.master_addresses) {
}

std::string KuduTableDescriptor::DebugString() const {
  std::stringstream out;
  out << "KuduTable(" << TableDescriptor::debug_string() << " table=" << table_name_;
  out << " master_addrs=[" << boost::join(master_addresses_, ",") << "]";
  out << " key_columns=[";
  out << join(key_columns_, ":");
  out << "])";
  return out.str();
}

MySQLTableDescriptor::MySQLTableDescriptor(const TTableDescriptor& tdesc)
    : TableDescriptor(tdesc),
      _mysql_db(tdesc.mysqlTable.db),
      _mysql_table(tdesc.mysqlTable.table),
      _host(tdesc.mysqlTable.host),
      _port(tdesc.mysqlTable.port),
      _user(tdesc.mysqlTable.user),
      _passwd(tdesc.mysqlTable.passwd) {
}

std::string MySQLTableDescriptor::debug_string() const {

    std::stringstream out;
    out << "MySQLTable(" << TableDescriptor::debug_string() << " _db" << _mysql_db << " table=" <<
        _mysql_table
        << " host=" << _host << " port=" << _port << " user=" << _user << " passwd=" << _passwd;
    return out.str();
}

TupleDescriptor::TupleDescriptor(const TTupleDescriptor& tdesc) :
        _id(tdesc.id),
        _table_desc(NULL),
        _byte_size(tdesc.byteSize),
        _num_null_bytes(tdesc.numNullBytes),
        _num_materialized_slots(0),
        _slots(),
        _has_varlen_slots(false),
        _llvm_struct(NULL) {
      if (false == tdesc.__isset.numNullSlots) {
        //be compatible for existing tables with no NULL value
        _num_null_slots = 0;
      } else {
        _num_null_slots = tdesc.numNullSlots;
      }
}

TupleDescriptor::TupleDescriptor(const PTupleDescriptor& pdesc)
        : _id(pdesc.id()),
        _table_desc(NULL),
        _byte_size(pdesc.byte_size()),
        _num_null_bytes(pdesc.num_null_bytes()),
        _num_materialized_slots(0),
        _slots(),
        _has_varlen_slots(false),
        _llvm_struct(NULL) {
    if (!pdesc.has_num_null_slots()) {
        //be compatible for existing tables with no NULL value
        _num_null_slots = 0;
    } else {
        _num_null_slots = pdesc.num_null_slots();
    }
}

void TupleDescriptor::add_slot(SlotDescriptor* slot) {
    _slots.push_back(slot);

    if (slot->is_materialized()) {
        ++_num_materialized_slots;

        if (slot->type().is_string_type()) {
            _string_slots.push_back(slot);
            _has_varlen_slots = true;
        } else {
            _no_string_slots.push_back(slot);
        }
    }
}

std::vector<SlotDescriptor*> TupleDescriptor::slots_ordered_by_idx() const {
    std::vector<SlotDescriptor*> sorted_slots(slots().size());
    for (SlotDescriptor* slot: slots()) {
        sorted_slots[slot->_slot_idx] = slot;
    }
    return sorted_slots;
}

bool TupleDescriptor::layout_equals(const TupleDescriptor& other_desc) const {
    if (byte_size() != other_desc.byte_size()) return false;
    if (slots().size() != other_desc.slots().size()) return false;

    std::vector<SlotDescriptor*> slots = slots_ordered_by_idx();
    std::vector<SlotDescriptor*> other_slots = other_desc.slots_ordered_by_idx();
    for (int i = 0; i < slots.size(); ++i) {
      if (!slots[i]->layout_equals(*other_slots[i])) return false;
    }
    return true;
}

void TupleDescriptor::to_protobuf(PTupleDescriptor* ptuple) const {
    ptuple->Clear();
    ptuple->set_id(_id);
    ptuple->set_byte_size(_byte_size);
    ptuple->set_num_null_bytes(_num_null_bytes);
    ptuple->set_table_id(-1);
    ptuple->set_num_null_slots(_num_null_slots);
}

std::string TupleDescriptor::debug_string() const {
    std::stringstream out;
    out << "Tuple(id=" << _id << " size=" << _byte_size;
    if (_table_desc != NULL) {
        //out << " " << _table_desc->debug_string();
    }

    out << " slots=[";
    for (size_t i = 0; i < _slots.size(); ++i) {
        if (i > 0) {
            out << ", ";
        }
        out << _slots[i]->debug_string();
    }

    out << "]";
    out << " has_varlen_slots=" << _has_varlen_slots;
    out << ")";
    return out.str();
}

RowDescriptor::RowDescriptor(
            const DescriptorTbl& desc_tbl,
            const std::vector<TTupleId>& row_tuples,
            const std::vector<bool>& nullable_tuples) :
        _tuple_idx_nullable_map(nullable_tuples) {
    DCHECK(nullable_tuples.size() == row_tuples.size());
    DCHECK_GT(row_tuples.size(), 0);
    _num_null_slots = 0;

    for (int i = 0; i < row_tuples.size(); ++i) {
        TupleDescriptor* tupleDesc = desc_tbl.get_tuple_descriptor(row_tuples[i]);
        _num_null_slots += tupleDesc->num_null_slots();
        _tuple_desc_map.push_back(tupleDesc);
        DCHECK(_tuple_desc_map.back() != NULL);
    }
    _num_null_bytes = (_num_null_slots + 7) / 8;

    init_tuple_idx_map();
    init_has_varlen_slots();
}

RowDescriptor::RowDescriptor(TupleDescriptor* tuple_desc, bool is_nullable) :
        _tuple_desc_map(1, tuple_desc),
        _tuple_idx_nullable_map(1, is_nullable) {
    init_tuple_idx_map();
    init_has_varlen_slots();
}

void RowDescriptor::init_tuple_idx_map() {
    // find max id
    TupleId max_id = 0;
    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        max_id = std::max(_tuple_desc_map[i]->id(), max_id);
    }

    _tuple_idx_map.resize(max_id + 1, INVALID_IDX);
    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        _tuple_idx_map[_tuple_desc_map[i]->id()] = i;
    }
}

void RowDescriptor::init_has_varlen_slots() {
    _has_varlen_slots = false;
    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        if (_tuple_desc_map[i]->has_varlen_slots()) {
            _has_varlen_slots = true;
            break;
        }
    }
}

int RowDescriptor::get_row_size() const {
    int size = 0;

    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        size += _tuple_desc_map[i]->byte_size();
    }

    return size;
}

int RowDescriptor::get_tuple_idx(TupleId id) const {
    DCHECK_LT(id, _tuple_idx_map.size()) << "RowDescriptor: " << debug_string();
    return _tuple_idx_map[id];
}

bool RowDescriptor::tuple_is_nullable(int tuple_idx) const {
    DCHECK_LT(tuple_idx, _tuple_idx_nullable_map.size()) << "RowDescriptor: " << debug_string();
    return _tuple_idx_nullable_map[tuple_idx];
}

bool RowDescriptor::is_any_tuple_nullable() const {
    for (int i = 0; i < _tuple_idx_nullable_map.size(); ++i) {
        if (_tuple_idx_nullable_map[i]) {
            return true;
        }
    }
    return false;
}

void RowDescriptor::to_thrift(std::vector<TTupleId>* row_tuple_ids) {
    row_tuple_ids->clear();

    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        row_tuple_ids->push_back(_tuple_desc_map[i]->id());
    }
}

void RowDescriptor::to_protobuf(
        google::protobuf::RepeatedField<google::protobuf::int32 >* row_tuple_ids) {
    row_tuple_ids->Clear();
    for (auto desc : _tuple_desc_map) {
        row_tuple_ids->Add(desc->id());
    }
}

bool RowDescriptor::is_prefix_of(const RowDescriptor& other_desc) const {
    if (_tuple_desc_map.size() > other_desc._tuple_desc_map.size()) {
        return false;
    }

    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        // pointer comparison okay, descriptors are unique
        if (_tuple_desc_map[i] != other_desc._tuple_desc_map[i]) {
            return false;
        }
    }

    return true;
}

bool RowDescriptor::equals(const RowDescriptor& other_desc) const {
    if (_tuple_desc_map.size() != other_desc._tuple_desc_map.size()) {
        return false;
    }

    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        // pointer comparison okay, descriptors are unique
        if (_tuple_desc_map[i] != other_desc._tuple_desc_map[i]) {
            return false;
        }
    }

    return true;
}

bool RowDescriptor::layout_is_prefix_of(const RowDescriptor& other_desc) const {
  if (_tuple_desc_map.size() > other_desc._tuple_desc_map.size()) return false;
  for (int i = 0; i < _tuple_desc_map.size(); ++i) {
    if (!_tuple_desc_map[i]->layout_equals(*other_desc._tuple_desc_map[i])) return false;
  }
  return true;
}

bool RowDescriptor::layout_equals(const RowDescriptor& other_desc) const {
    if (_tuple_desc_map.size() != other_desc._tuple_desc_map.size()) return false;
    return layout_is_prefix_of(other_desc);
}

std::string RowDescriptor::debug_string() const {
    std::stringstream ss;

    ss << "tuple_desc_map: [";
    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        ss << _tuple_desc_map[i]->debug_string();
        if (i != _tuple_desc_map.size() -1) {
            ss << ", ";
        }
    }
    ss << "] ";

    ss << "tuple_id_map: [";
    for (int i = 0; i < _tuple_idx_map.size(); ++i) {
        ss << _tuple_idx_map[i];
        if (i != _tuple_idx_map.size() -1) {
            ss << ", ";
        }
    }
    ss << "] ";

    ss << "tuple_is_nullable: [";
    for (int i = 0; i < _tuple_idx_nullable_map.size(); ++i) {
        ss << _tuple_idx_nullable_map[i];
        if (i != _tuple_idx_nullable_map.size() -1) {
            ss << ", ";
        }
    }
    ss << "] ";

    return ss.str();
}

Status DescriptorTbl::create(ObjectPool* pool, const TDescriptorTable& thrift_tbl,
                             DescriptorTbl** tbl) {
    *tbl = pool->add(new DescriptorTbl());

    // deserialize table descriptors first, they are being referenced by tuple descriptors
    for (size_t i = 0; i < thrift_tbl.tableDescriptors.size(); ++i) {
        const TTableDescriptor& tdesc = thrift_tbl.tableDescriptors[i];
        TableDescriptor* desc = NULL;

        switch (tdesc.tableType) {
        case TTableType::MYSQL_TABLE:
            desc = pool->add(new MySQLTableDescriptor(tdesc));
            break;

        case TTableType::OLAP_TABLE:
            desc = pool->add(new OlapTableDescriptor(tdesc));
            break;

        case TTableType::SCHEMA_TABLE:
            desc = pool->add(new SchemaTableDescriptor(tdesc));
            break;
        case TTableType::BROKER_TABLE:
            desc = pool->add(new BrokerTableDescriptor(tdesc));
            break;
        default:
            DCHECK(false) << "invalid table type: " << tdesc.tableType;
        }

        (*tbl)->_tbl_desc_map[tdesc.id] = desc;
    }

    for (size_t i = 0; i < thrift_tbl.tupleDescriptors.size(); ++i) {
        const TTupleDescriptor& tdesc = thrift_tbl.tupleDescriptors[i];
        TupleDescriptor* desc = pool->add(new TupleDescriptor(tdesc));

        // fix up table pointer
        if (tdesc.__isset.tableId) {
            desc->_table_desc = (*tbl)->get_table_descriptor(tdesc.tableId);
            DCHECK(desc->_table_desc != NULL);
        }

        (*tbl)->_tuple_desc_map[tdesc.id] = desc;
    }

    for (size_t i = 0; i < thrift_tbl.slotDescriptors.size(); ++i) {
        const TSlotDescriptor& tdesc = thrift_tbl.slotDescriptors[i];
        SlotDescriptor* slot_d = pool->add(new SlotDescriptor(tdesc));
        (*tbl)->_slot_desc_map[tdesc.id] = slot_d;

        // link to parent
        TupleDescriptorMap::iterator entry = (*tbl)->_tuple_desc_map.find(tdesc.parent);

        if (entry == (*tbl)->_tuple_desc_map.end()) {
            return Status("unknown tid in slot descriptor msg");
        }

        entry->second->add_slot(slot_d);
    }

    return Status::OK;
}

TableDescriptor* DescriptorTbl::get_table_descriptor(TableId id) const {
    // TODO: is there some boost function to do exactly this?
    TableDescriptorMap::const_iterator i = _tbl_desc_map.find(id);

    if (i == _tbl_desc_map.end()) {
        return NULL;
    } else {
        return i->second;
    }
}

TupleDescriptor* DescriptorTbl::get_tuple_descriptor(TupleId id) const {
    // TODO: is there some boost function to do exactly this?
    TupleDescriptorMap::const_iterator i = _tuple_desc_map.find(id);

    if (i == _tuple_desc_map.end()) {
        return NULL;
    } else {
        return i->second;
    }
}

SlotDescriptor* DescriptorTbl::get_slot_descriptor(SlotId id) const {
    // TODO: is there some boost function to do exactly this?
    SlotDescriptorMap::const_iterator i = _slot_desc_map.find(id);

    if (i == _slot_desc_map.end()) {
        return NULL;
    } else {
        return i->second;
    }
}

// return all registered tuple descriptors
void DescriptorTbl::get_tuple_descs(std::vector<TupleDescriptor*>* descs) const {
    descs->clear();

    for (TupleDescriptorMap::const_iterator i = _tuple_desc_map.begin();
            i != _tuple_desc_map.end(); ++i) {
        descs->push_back(i->second);
    }
}

bool SlotDescriptor::layout_equals(const SlotDescriptor& other_desc) const {
    if (type().type != other_desc.type().type) return false;
    if (is_nullable() != other_desc.is_nullable()) return false;
    if (slot_size() != other_desc.slot_size()) return false;
    if (tuple_offset() != other_desc.tuple_offset()) return false;
    if (!null_indicator_offset().equals(other_desc.null_indicator_offset())) return false;
  return true;
}

// Generate function to check if a slot is null.  The resulting IR looks like:
// (in this case the tuple contains only a nullable double)
// define i1 @IsNull({ i8, double }* %tuple) {
// entry:
//   %null_byte_ptr = getelementptr inbounds { i8, double }* %tuple, i32 0, i32 0
//   %null_byte = load i8* %null_byte_ptr
//   %null_mask = and i8 %null_byte, 1
//   %is_null = icmp ne i8 %null_mask, 0
//   ret i1 %is_null
// }
llvm::Function* SlotDescriptor::codegen_is_null(LlvmCodeGen* codegen, llvm::StructType* tuple) {
    if (_is_null_fn != NULL) {
        return _is_null_fn;
    }

    llvm::PointerType* tuple_ptr_type = llvm::PointerType::get(tuple, 0);
    LlvmCodeGen::FnPrototype prototype(codegen, "IsNull", codegen->get_type(TYPE_BOOLEAN));
    prototype.add_argument(LlvmCodeGen::NamedVariable("tuple", tuple_ptr_type));

    llvm::Value* mask = codegen->get_int_constant(TYPE_TINYINT, _null_indicator_offset.bit_mask);
    llvm::Value* zero = codegen->get_int_constant(TYPE_TINYINT, 0);
    int byte_offset = _null_indicator_offset.byte_offset;

    LlvmCodeGen::LlvmBuilder builder(codegen->context());
    llvm::Value* tuple_ptr = NULL;
    llvm::Function* fn = prototype.generate_prototype(&builder, &tuple_ptr);

    llvm::Value* null_byte_ptr = builder.CreateStructGEP(tuple_ptr, byte_offset, "null_byte_ptr");
    llvm::Value* null_byte = builder.CreateLoad(null_byte_ptr, "null_byte");
    llvm::Value* null_mask = builder.CreateAnd(null_byte, mask, "null_mask");
    llvm::Value* is_null = builder.CreateICmpNE(null_mask, zero, "is_null");
    builder.CreateRet(is_null);

    return _is_null_fn = codegen->finalize_function(fn);
}

// Generate function to set a slot to be null or not-null.  The resulting IR
// for SetNotNull looks like:
// (in this case the tuple contains only a nullable double)
// define void @SetNotNull({ i8, double }* %tuple) {
// entry:
//   %null_byte_ptr = getelementptr inbounds { i8, double }* %tuple, i32 0, i32 0
//   %null_byte = load i8* %null_byte_ptr
//   %0 = and i8 %null_byte, -2
//   store i8 %0, i8* %null_byte_ptr
//   ret void
// }
llvm::Function* SlotDescriptor::codegen_update_null(LlvmCodeGen* codegen,
        llvm::StructType* tuple, bool set_null) {
    if (set_null && _set_null_fn != NULL) {
        return _set_null_fn;
    }

    if (!set_null && _set_not_null_fn != NULL) {
        return _set_not_null_fn;
    }

    llvm::PointerType* tuple_ptr_type = llvm::PointerType::get(tuple, 0);
    LlvmCodeGen::FnPrototype prototype(codegen, (set_null) ? "SetNull" : "SetNotNull",
                                       codegen->void_type());
    prototype.add_argument(LlvmCodeGen::NamedVariable("tuple", tuple_ptr_type));

    LlvmCodeGen::LlvmBuilder builder(codegen->context());
    llvm::Value* tuple_ptr = NULL;
    llvm::Function* fn = prototype.generate_prototype(&builder, &tuple_ptr);

    llvm::Value* null_byte_ptr =
        builder.CreateStructGEP(
            tuple_ptr, _null_indicator_offset.byte_offset, "null_byte_ptr");
    llvm::Value* null_byte = builder.CreateLoad(null_byte_ptr, "null_byte");
    llvm::Value* result = NULL;

    if (set_null) {
        llvm::Value* null_set = codegen->get_int_constant(
                              TYPE_TINYINT, _null_indicator_offset.bit_mask);
        result = builder.CreateOr(null_byte, null_set);
    } else {
        llvm::Value* null_clear_val =
            codegen->get_int_constant(TYPE_TINYINT, ~_null_indicator_offset.bit_mask);
        result = builder.CreateAnd(null_byte, null_clear_val);
    }

    builder.CreateStore(result, null_byte_ptr);
    builder.CreateRetVoid();

    fn = codegen->finalize_function(fn);

    if (set_null) {
        _set_null_fn = fn;
    } else {
        _set_not_null_fn = fn;
    }

    return fn;
}

// The default llvm packing is identical to what we do in the FE.  Each field is aligned
// to begin on the size for that type.
// TODO: Understand llvm::SetTargetData which allows you to explicitly define the packing
// rules.
llvm::StructType* TupleDescriptor::generate_llvm_struct(LlvmCodeGen* codegen) {
    // If we already generated the llvm type, just return it.
    if (_llvm_struct != NULL) {
        return _llvm_struct;
    }

    // For each null byte, add a byte to the struct
    std::vector<llvm::Type*> struct_fields;
    struct_fields.resize(_num_null_bytes + _num_materialized_slots);

    for (int i = 0; i < _num_null_bytes; ++i) {
        struct_fields[i] = codegen->get_type(TYPE_TINYINT);
    }

    // Add the slot types to the struct description.
    for (int i = 0; i < slots().size(); ++i) {
        SlotDescriptor* slot_desc = slots()[i];

        if (slot_desc->is_materialized()) {
            slot_desc->_field_idx = slot_desc->_slot_idx + _num_null_bytes;
            DCHECK_LT(slot_desc->field_idx(), struct_fields.size());
            struct_fields[slot_desc->field_idx()] = codegen->get_type(slot_desc->type().type);
        }
    }

    // Construct the struct type.
    llvm::StructType* tuple_struct = llvm::StructType::get(
        codegen->context(), llvm::ArrayRef<llvm::Type*>(struct_fields));

    // Verify the alignment is correct.  It is essential that the layout matches
    // identically.  If the layout does not match, return NULL indicating the
    // struct could not be codegen'd.  This will trigger codegen for anything using
    // the tuple to be disabled.
    const llvm::DataLayout* data_layout = codegen->execution_engine()->getDataLayout();
    const llvm::StructLayout* layout = data_layout->getStructLayout(tuple_struct);
    layout = data_layout->getStructLayout(tuple_struct);

    if (layout->getSizeInBytes() != byte_size()) {
        DCHECK_EQ(layout->getSizeInBytes(), byte_size());
        return NULL;
    }

    for (int i = 0; i < slots().size(); ++i) {
        SlotDescriptor* slot_desc = slots()[i];

        if (slot_desc->is_materialized()) {
            int field_idx = slot_desc->field_idx();

            // Verify that the byte offset in the llvm struct matches the tuple offset
            // computed in the FE
            if (layout->getElementOffset(field_idx) != slot_desc->tuple_offset()) {
                DCHECK_EQ(layout->getElementOffset(field_idx), slot_desc->tuple_offset());
                return NULL;
            }
        }
    }

    _llvm_struct = tuple_struct;
    return tuple_struct;
}

std::string DescriptorTbl::debug_string() const {
    std::stringstream out;
    out << "tuples:\n";

    for (TupleDescriptorMap::const_iterator i = _tuple_desc_map.begin();
            i != _tuple_desc_map.end(); ++i) {
        out << i->second->debug_string() << '\n';
    }

    return out.str();
}

}