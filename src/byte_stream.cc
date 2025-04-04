#include "byte_stream.hh"

using namespace std;

ByteStream::ByteStream( uint64_t capacity ) : capacity_( capacity ) {}

void Writer::push( string data )
{
  uint64_t n = std::min( available_capacity(), data.size() );
  if ( n == 0 ) {
    return;
  }
  buffer_.push_back( data.substr( 0, n ) );
  bytes_pushed_ += n;
}

void Writer::close()
{
  close_ = true;
}

bool Writer::is_closed() const
{
  return close_;
}

uint64_t Writer::available_capacity() const
{
  return capacity_ - ( bytes_pushed_ - bytes_popped_ );
}

uint64_t Writer::bytes_pushed() const
{
  return bytes_pushed_;
}

string_view Reader::peek() const
{
  return buffer_.front();
}

void Reader::pop( uint64_t len )
{
  uint64_t n = std::min( bytes_buffered(), len );
  if ( n == 0 ) {
    return;
  }
  uint64_t i = 0;
  while (i < n) {
    uint64_t bytes_size = buffer_.front().size();
    uint64_t popped_size = std::min(bytes_size, n - i);
    if (popped_size == bytes_size) {
      buffer_.pop_front();
    } else {
      auto& bytes = buffer_.front();
      bytes = bytes.substr(popped_size);
    }
    i += popped_size;
  }
  bytes_popped_ += n;
}

bool Reader::is_finished() const
{
  return close_ and bytes_buffered() == 0;
}

uint64_t Reader::bytes_buffered() const
{
  return bytes_pushed_ - bytes_popped_;
}

uint64_t Reader::bytes_popped() const
{
  return bytes_popped_;
}