#include "reassembler.hh"
#include "debug.hh"

using namespace std;

void Reassembler::insert( uint64_t first_index, string data, bool is_last_substring )
{
  Writer& writer = output_.writer();
  uint64_t unassembled_index = writer.bytes_pushed();
  uint64_t available_capacity = writer.available_capacity();
  uint64_t unacceptable_index = unassembled_index + available_capacity;
  uint64_t begin_index = max( first_index, unassembled_index );
  uint64_t end_index = min( first_index + data.size(), unacceptable_index );
  last_ = last_ or ( is_last_substring and first_index + data.size() <= unacceptable_index );
  if ( end_index <= begin_index ) {
    should_close_writer();
    return;
  }
  auto it = storage_.lower_bound( begin_index );
  if ( it != storage_.begin() ) {
    --it;
    uint64_t last_end_index = it->first + it->second.size();
    if ( end_index <= last_end_index ) {
      should_close_writer();
      return;
    }
    begin_index = max( begin_index, last_end_index );
    ++it;
  }
  while ( it != storage_.end() ) {
    uint64_t it_begin_index = it->first;
    uint64_t it_end_index = it->first + it->second.size();
    if ( it_begin_index <= begin_index and end_index <= it_end_index ) {
      should_close_writer();
      return;
    }
    if ( it_end_index <= end_index ) {
      it = storage_.erase( it );
    } else {
      end_index = min( end_index, it_begin_index );
      break;
    }
  }
  storage_[begin_index] = data.substr( begin_index - first_index, end_index - begin_index );
  it = storage_.begin();
  while ( it != storage_.end() and writer.bytes_pushed() == it->first ) {
    writer.push( it->second );
    it = storage_.erase( it );
  }
  should_close_writer();
}

// How many bytes are stored in the Reassembler itself?
// This function is for testing only; don't add extra state to support it.
uint64_t Reassembler::count_bytes_pending() const
{
  uint64_t count = 0;
  for ( const auto& kv : storage_ ) {
    count += kv.second.size();
  }
  return count;
}

void Reassembler::should_close_writer()
{
  Writer& writer = output_.writer();
  if ( last_ and storage_.empty() )
    writer.close();
}
