/*
 * Author: Wade Spires
 *
 * Description:
 * Exchange order matching engine.
 *
 * Organization:
 * 1. Data Types - Definitions for basic data types, such as side, price, etc.
 * 2. Message Types - Message structures with normalized types to handle each operation.
 * 3. Order Book - Order book made up of separate sets of buy and sell levels ordered by price where each level has a queue of orders.
 * 4. Matching Engine - Matchine engine dispatches events to the order book and handles trade events.
 * 5. Command Processor - Reads and dispatches commands to the matching engine.
 * 6. Main - Make and run the command processor with a matching engine using stdin and stdout streams.
 * 7. Unit Tests - Tests matching engine with various inputs.
 *
 * Improvements:
 * 1. Fix-sized OrderID - Use a fixed-size array internally for OrderID to avoid string allocations (requires an upper bound in order ID length in the spec).
 * 2. Threading - Read and parse input from one thread and run matching engine in a separate thread.
 * 3. Intrusive Container - Use Boost intrusive containers for list and set to store objects themselves in containers instead of dynamically allocated copies.
 * 4. Memory Pool - Allocate orders, levels, etc. using a memory pool.
 * 5. Uncross Book - Add an uncross method to Book that calculates all trades for a crossed book, such as during an auction.
 * 6. Unordered Prices - Use unordered_set in addition to set to hold pointers to levels by price for O(1) lookup.
 */
#include <algorithm>
#include <atomic>
#include <cassert>
#include <cctype>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <functional>
#include <iterator>
#include <fstream>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <queue>
#include <set>
#include <stdexcept>
#include <string>
#include <sstream>
#include <unordered_map>
#include <thread>
#include <utility>
#include <vector>

// Define DEBUG macro to enable more verbose logging and checks within IF_DEBUG.
//#define DEBUG
#ifdef DEBUG
    #define IF_DEBUG(x) {x}
#else
    #define IF_DEBUG(x)
#endif


/*
 * 1. Data Types - Definitions for basic data types, such as side, price, etc.
 */

enum class Side : char
{
    Buy = 'B',
    Sell = 'S',
    Invalid = '?',
};

std::ostream & operator<<(std::ostream & os, Side side)
{
    switch (side)
    {
        case Side::Buy:
        {
            os << "BUY";
            break;
        }

        case Side::Sell:
        {
            os << "SELL";
            break;
        }

        case Side::Invalid:
        {
            os << "INVALID";
            break;
        }
    }
    return os;
}

std::istream & operator>>(std::istream & is, Side & side)
{
    // Use str as intermediate result for comparison to determine the enum value.
    // Make static to avoid allocation for each invocation.
    // Make thread_local (which implies static, but making it explicit) for thread-safety.
    // Construct with n null characters to have an initial capacity to hold the max expected input without reallocation.
    thread_local static std::string str{4, '\0'};
    is >> str;
    if (str == "BUY")
    {
        side = Side::Buy;
    }
    else if (str == "SELL")
    {
        side = Side::Sell;
    }
    else
    {
        side = Side::Invalid;
    }
    return is;
}


enum class TIF : char
{
    GFD = '0', // Good For Day: Order will stay in the order book until it's been all traded
    IOC = '3', // Immediate Or Cancel: If order can't be traded immediately, it will be cancelled right away. If only partially traded, non-traded part is cancelled.
    Invalid = '?',
};

std::ostream & operator<<(std::ostream & os, TIF tif)
{
    switch (tif)
    {
        case TIF::GFD:
        {
            os << "GFD";
            break;
        }

        case TIF::IOC:
        {
            os << "IOC";
            break;
        }

        case TIF::Invalid:
        {
            os << "INVALID";
            break;
        }
    }
    return os;
}

std::istream & operator>>(std::istream & is, TIF & tif)
{
    thread_local static std::string str{3, '\0'};
    is >> str;
    if (str == "GFD")
    {
        tif = TIF::GFD;
    }
    else if (str == "IOC")
    {
        tif = TIF::IOC;
    }
    else
    {
        tif = TIF::Invalid;
    }
    return is;
}


// Price data type that wraps unsigned integer.
// Not simply a typedef so that we have strong type-checking and can more easily add features, such as support decimal prices.
class Price
{
public:
    // Underlying storage type.
    using value_type = std::uint64_t;

    // Ctors
    Price() = default;
    explicit Price(value_type value)
        : value_{value}
    {
    }

    void value(value_type val) noexcept { value_ = val; }
    value_type value() const noexcept { return value_; }

    bool is_zero() const noexcept { return value_ == 0; }

    // Comparison ops
    bool operator==(Price rhs) const { return value_ == rhs.value_; }
    bool operator!=(Price rhs) const { return not (*this == rhs); }
    bool operator< (Price rhs) const { return value_ < rhs.value_; }
    bool operator> (Price rhs) const { return rhs < *this; }
    bool operator<=(Price rhs) const { return not (*this > rhs); }
    bool operator>=(Price rhs) const { return not (*this < rhs); }

    // Arithmetic ops
    Price & operator+=(Price rhs)
    {
        value_ += rhs.value_;
        return *this;
    }
    friend Price operator+(Price lhs, Price rhs)
    {
        lhs += rhs;
        return lhs;
    }

    Price & operator-=(Price rhs)
    {
        assert(value_ >= rhs.value_); // Required since value is unsigned.
        value_ -= rhs.value_;
        return *this;
    }
    friend Price operator-(Price lhs, Price rhs)
    {
        lhs -= rhs;
        return lhs;
    }

private:
    value_type value_ = 0;
};

std::ostream & operator<<(std::ostream & os, Price price)
{
    return os << price.value();
}

std::istream & operator>>(std::istream & is, Price & price)
{
    Price::value_type value{};
    is >> value;
    price.value(value);
    return is;
}


class Qty
{
public:
    // Underlying storage type.
    using value_type = std::uint64_t;

    // Ctors
    Qty() = default;
    explicit Qty(value_type value)
        : value_{value}
    {
    }

    void value(value_type val) noexcept { value_ = val; }
    value_type value() const noexcept { return value_; }

    bool is_zero() const noexcept { return value_ == 0; }

    // Comparison ops
    bool operator==(Qty rhs) const { return value_ == rhs.value_; }
    bool operator!=(Qty rhs) const { return not (*this == rhs); }
    bool operator< (Qty rhs) const { return value_ < rhs.value_; }
    bool operator> (Qty rhs) const { return rhs < *this; }
    bool operator<=(Qty rhs) const { return not (*this > rhs); }
    bool operator>=(Qty rhs) const { return not (*this < rhs); }

    // Arithmetic ops
    Qty & operator+=(Qty rhs)
    {
        value_ += rhs.value_;
        return *this;
    }
    friend Qty operator+(Qty lhs, Qty rhs)
    {
        lhs += rhs;
        return lhs;
    }

    Qty & operator-=(Qty rhs)
    {
        assert(value_ >= rhs.value_); // Required since value is unsigned.
        value_ -= rhs.value_;
        return *this;
    }
    friend Qty operator-(Qty lhs, Qty rhs)
    {
        lhs -= rhs;
        return lhs;
    }

private:
    value_type value_ = 0;
};

std::ostream & operator<<(std::ostream & os, Qty qty)
{
    return os << qty.value();
}

std::istream & operator>>(std::istream & is, Qty & qty)
{
    Qty::value_type value{};
    is >> value;
    qty.value(value);
    return is;
}


class OrderID
{
public:
    // Underlying storage type.
    // Using a std::string to handle arbitrary length words,
    // but this is expensive due to string memory allocations.
    // Should otherwise use a fixed-size array if the max length is known
    // or just an integer type.
    using value_type = std::string;

    OrderID() = default;
    explicit OrderID(value_type value)
        : value_{std::move(value)} // Copy-move idiom
    {
    }

    void value(value_type const & val) noexcept { value_ = val; }
    value_type const & value() const noexcept { return value_; }
    value_type & value() noexcept { return value_; }

    bool empty() const noexcept { return value_.empty(); }

    // Comparison ops
    bool operator==(OrderID rhs) const { return value_ == rhs.value_; }
    bool operator!=(OrderID rhs) const { return not (*this == rhs); }
    bool operator< (OrderID rhs) const { return value_ < rhs.value_; }
    bool operator> (OrderID rhs) const { return rhs < *this; }
    bool operator<=(OrderID rhs) const { return not (*this > rhs); }
    bool operator>=(OrderID rhs) const { return not (*this < rhs); }

private:
    value_type value_ = {};
};

std::ostream & operator<<(std::ostream & os, OrderID const & order_id)
{
    return os << order_id.value();
}

std::istream & operator>>(std::istream & is, OrderID & order_id)
{
    is >> order_id.value();
    return is;
}

// std hash specialization to use in unordered_map.
namespace std {

template <>
struct hash<OrderID>
{
    std::size_t operator()(OrderID const & order_id) const
    {
        return std::hash<OrderID::value_type>()(static_cast<OrderID::value_type>(order_id.value()));
    }
};

}


/*
 * 2. Message Types - Message structures with normalized types to handle each operation.
 */

struct BuyOrder
{
    TIF tif;
    Price price;
    Qty qty;
    OrderID order_id;

    bool is_invalid() const
    {
        return tif == TIF::Invalid
            or price.is_zero()
            or qty.is_zero()
            or order_id.empty()
            ;
    }
    bool is_valid() const { return not is_invalid(); }
};

std::ostream & operator<<(std::ostream & os, BuyOrder const & msg)
{
    return os << "BUY "
        << msg.tif
        << ' ' << msg.price
        << ' ' << msg.qty
        << ' ' << msg.order_id
        ;
}

std::istream & operator>>(std::istream & is, BuyOrder & msg)
{
    return is >> msg.tif
        >> msg.price
        >> msg.qty
        >> msg.order_id
        ;
}


struct SellOrder
{
    TIF tif;
    Price price;
    Qty qty;
    OrderID order_id;

    bool is_invalid() const
    {
        return tif == TIF::Invalid
            or price.is_zero()
            or qty.is_zero()
            or order_id.empty()
            ;
    }
    bool is_valid() const { return not is_invalid(); }
};

std::ostream & operator<<(std::ostream & os, SellOrder const & msg)
{
    return os << "SELL "
        << msg.tif
        << ' ' << msg.price
        << ' ' << msg.qty
        << ' ' << msg.order_id
        ;
}

std::istream & operator>>(std::istream & is, SellOrder & msg)
{
    return is >> msg.tif
        >> msg.price
        >> msg.qty
        >> msg.order_id
        ;
}


struct CancelOrder
{
    OrderID order_id;

    bool is_invalid() const
    {
        return order_id.empty();
    }
    bool is_valid() const { return not is_invalid(); }
};

std::ostream & operator<<(std::ostream & os, CancelOrder const & msg)
{
    return os << "CANCEL "
        << msg.order_id
        ;
}

std::istream & operator>>(std::istream & is, CancelOrder & msg)
{
    return is >> msg.order_id;
}


struct ModifyOrder
{
    OrderID order_id;
    Side side;
    Price price;
    Qty qty;

    bool is_invalid() const
    {
        return order_id.empty()
            or side == Side::Invalid
            or price.is_zero()
            or qty.is_zero()
            ;
    }
    bool is_valid() const { return not is_invalid(); }
};

std::ostream & operator<<(std::ostream & os, ModifyOrder const & msg)
{
    return os << "MODIFY "
        << msg.order_id
        << ' ' << msg.side
        << ' ' << msg.price
        << ' ' << msg.qty
        ;
}

std::istream & operator>>(std::istream & is, ModifyOrder & msg)
{
    return is >> msg.order_id
        >> msg.side
        >> msg.price
        >> msg.qty
        ;
}


struct PrintBook
{
    bool is_invalid() const { return false; }
    bool is_valid() const { return not is_invalid(); }
};

std::ostream & operator<<(std::ostream & os, PrintBook const &)
{
    return os << "PRINT";
}

std::istream & operator>>(std::istream & is, PrintBook & msg)
{
    return is;
}


// Clear all orders from book. Not required, but useful to have.
struct ClearBook
{
    bool is_invalid() const { return false; }
    bool is_valid() const { return not is_invalid(); }
};

std::ostream & operator<<(std::ostream & os, ClearBook const &)
{
    return os << "CLEAR";
}

std::istream & operator>>(std::istream & is, ClearBook & msg)
{
    return is;
}


/*
 * 3. Order Book - Order book made up of separate sets of buy and sell levels ordered by price where each level has a queue of orders.
 */

// Forward decl
class Level;
class Book;

class Order
{
public:
    Order(OrderID order_id, Qty qty)
        : order_id_{std::move(order_id)}
        , qty_{qty}
    {
    }

    OrderID const & order_id() const noexcept { return order_id_; }

    Qty qty() const noexcept { return qty_; }
    Order & qty(Qty q) { qty_ = q; return *this; }

    // Comparison ops
    bool operator==(Order const & rhs) const { return order_id_ == rhs.order_id_; }
    bool operator!=(Order const & rhs) const { return not (*this == rhs); }

private:
    OrderID order_id_;
    Qty qty_;

    // Level in which this order resides and iterator pointing to this order.
    // Level and Book use these to quickly access this order instead of searching for it (O(1) instead of O(n)).
    friend Level;
    friend Book;
    using Queue = std::list<Order>;
    Level * level_ = nullptr;
    Queue::iterator iterator_;
};

std::ostream & operator<<(std::ostream & os, Order const & order)
{
    return os << order.order_id()
        << ':' << order.qty()
        ;
}


class Level
{
public:
    explicit Level(Price price = Price{})
        : price_{price}
    {
    }

    Qty qty() const noexcept { return qty_; }
    Price price() const { return price_; }
    Order::Queue const & orders() const { return orders_; }

    bool empty() const noexcept { return orders_.empty(); }
    std::size_t size() const noexcept { return orders_.size(); }

    Order::Queue::iterator add(OrderID order_id, Qty qty)
    {
        // Append order to end of level and save this iterator to the order.
        orders_.emplace_back(std::move(order_id), qty);
        auto iter = --orders_.end();
        iter->level_ = this;
        iter->iterator_ = iter;
        qty_ += qty;
        return iter;
    }

    void cancel(Order & order)
    {
        // Erase order from this level using its saved iterator.
        assert(order.level_ == this);
        assert(&(*order.iterator_) == &order); // Must be the same order instance.
        qty_ -= order.qty();
        orders_.erase(order.iterator_);
    }

    // Modify order. The order loses its queue position by being pushed to the end.
    void modify(Order & order, Qty qty)
    {
        modify_qty(order, qty);

        // Transfer order to the back of the queue.
        // Note: No elements are copied or moved, only the internal pointers of the list nodes are re-pointed, and
        // no iterators become invalidated.
        orders_.splice(orders_.end(), orders_, order.iterator_);
    }

    // Modify order qty. The order keeps its position in the queue.
    void modify_qty(Order & order, Qty qty)
    {
        // Modify level qty and assign new order qty.
        assert(not qty.is_zero());
        assert(order.level_ == this);
        assert(&(*order.iterator_) == &order); // Must be the same order instance.
        qty_ -= order.qty();
        qty_ += qty;
        order.qty(qty);
    }

    // Write all orders in this level.
    void write_orders(std::ostream & os) const
    {
        os << size() << ':' << qty() << " @ " << price() << ":[";
        for (auto && order : orders())
        {
            os << order << ' ';
        }
        os << ']';
    }

private:
    Qty qty_ = Qty{};
    Price price_ = Price{};
    Order::Queue orders_ = {};

    // Level set in which this level resides and iterator pointing to this level.
    // Book uses these to quickly access this instead of searching for it (O(1) instead of O(log n)).
    friend Book;
    struct CompareLevel
    {
        // Functor to compare between Level and Price types.
        // Note: this is made an inner class because we need this definition for the Set type and members.
        // Comparison is lhs > rhs for decreasing order.

        // Allow calling set functions without constructing an instance of key (heterogeneous lookup).
        using is_transparent = void;

        bool operator()(Level const & lhs, Level const & rhs) const
        {
            return lhs.price() > rhs.price();
        }
        bool operator()(Price lhs, Level const & rhs) const
        {
            return lhs > rhs.price();
        }
        bool operator()(Level const & lhs, Price rhs) const
        {
            return lhs.price() > rhs;
        }
    };
    using Set = std::set<Level, Level::CompareLevel>;
    Set * levels_ = nullptr;
    Set::iterator iterator_;
};

std::ostream & operator<<(std::ostream & os, Level const & level)
{
    return os << level.price() << ' ' << level.qty();
}


// Trade event from matching a passive order in the book with an incoming aggressive order.
struct Trade
{
    Price passive_price;
    Order passive_order;
    Price aggressive_price;
    Order aggressive_order;
};

std::ostream & operator<<(std::ostream & os, Trade const & trade)
{
    return os << "TRADE "
        << trade.passive_order.order_id()
        << ' ' << trade.passive_price
        << ' ' << trade.passive_order.qty()
        << ' ' << trade.aggressive_order.order_id()
        << ' ' << trade.aggressive_price
        << ' ' << trade.aggressive_order.qty()
        ;
}

using Trades = std::vector<Trade>;

std::ostream & operator<<(std::ostream & os, Trades const & trades)
{
    for (auto && trade : trades)
    {
        os << trade << std::endl;
    }
    return os;
}


class Book
{
public:
    Level::Set const & buy_levels() const { return buy_levels_; }
    Level::Set const & sell_levels() const { return sell_levels_; }

    void add(Side side, OrderID const & order_id, Qty qty, Price price)
    {
        switch (side)
        {
            case Side::Buy:
            {
                add(order_id, qty, price, buy_levels_);
                break;
            }

            case Side::Sell:
            {
                add(order_id, qty, price, sell_levels_);
                break;
            }

            case Side::Invalid:
            {
                break;
            }
        }
    }

    void cancel(OrderID const & order_id)
    {
        auto iter = orders_by_id_.find(order_id);
        if (iter == orders_by_id_.end())
        {
            // Should not happen since implies erasing an order never added.
            IF_DEBUG(
                std::cerr << "Unable to cancel unknown order: " << order_id << std::endl;
                //assert(false);
            )
            return; // Ignoring for now.
        }

        // Get order and level it's contained within.
        // Erase order from the level.
        // Erase the level if empty.
        auto && order_iter = iter->second;
        auto && order = *order_iter;
        assert(order.level_);
        auto && level = *order.level_;
        level.cancel(order);
        if (level.empty())
        {
            // Erase empty level using its internally held container and iter.
            assert(level.levels_);
            auto && levels = *level.levels_;
            levels.erase(level.iterator_);
        }
        orders_by_id_.erase(iter);
    }

    void modify(Side side, OrderID const & order_id, Qty qty, Price price)
    {
        switch (side)
        {
            case Side::Buy:
            {
                modify(order_id, qty, price, buy_levels_);
                break;
            }

            case Side::Sell:
            {
                modify(order_id, qty, price, sell_levels_);
                break;
            }

            case Side::Invalid:
            {
                break;
            }
        }
    }

    void clear()
    {
        buy_levels_.clear();
        sell_levels_.clear();
        orders_by_id_.clear();
    }

    // Match order with orders in this book.
    // The matched passive orders are removed from the book, paired with the aggressive order, and saved in the output list of trades.
    // Returns leaves_qty, the remaining quantity left after all matching (leaves_qty >= 0).
    Qty match(Side side, OrderID const & order_id, Qty qty, Price price, Trades & trades)
    {
        Qty leaves_qty = qty;
        switch (side)
        {
            case Side::Buy:
            {
                // Match buy with sells.
                // Sells are in decreasing order (highest price first), so iterate in reverse to start with lowest price.
                leaves_qty = match(side, order_id, qty, price, sell_levels_.crbegin(), sell_levels_.crend(), trades,
                    [](Price order_price, Price level_price) -> bool
                    {
                        return order_price >= level_price;
                    });
                break;
            }

            case Side::Sell:
            {
                // Match sell with buys.
                leaves_qty = match(side, order_id, qty, price, buy_levels_.cbegin(), buy_levels_.cend(), trades,
                    [](Price order_price, Price level_price) -> bool
                    {
                        return order_price <= level_price;
                    });
                break;
            }

            case Side::Invalid:
            {
                break;
            }
        }

        fill_orders(trades);
        return leaves_qty;
    }

    // Write all orders in the book.
    void write_orders(std::ostream & os) const
    {
        os << "SELL:\n";
        for (auto && level : sell_levels_)
        {
            level.write_orders(os);
            os << '\n';
        }

        os << "BUY:\n";
        for (auto && level : buy_levels_)
        {
            level.write_orders(os);
            os << '\n';
        }

        os << std::endl;
    }

protected:
    void add(OrderID const & order_id, Qty qty, Price price, Level::Set & levels)
    {
        if (orders_by_id_.count(order_id))
        {
            IF_DEBUG(
                std::cerr << "Unable to add duplicate order: " << order_id << std::endl;
                //assert(false);
            )
            return;
        }

        // Search for level with given price, inserting it if it does not exist using hint.
        auto level_iter = levels.lower_bound(price);
        if (level_iter == levels.end() or level_iter->price() != price)
        {
            level_iter = levels.emplace_hint(
                  level_iter
                , price
                );

            // Note: const_cast required because std::set iter is const since could otherwise modify comparison order
            // externally from the set. We do not modify the level price, so it is safe.
            // Level saves its container and iterator to allow erasing using only the order ID.
            Level & level = const_cast<Level &>(*level_iter);
            level.levels_ = &levels;
            level.iterator_ = level_iter;
        }

        // Add order to the level and map.
        Level & level = const_cast<Level &>(*level_iter);
        auto order_iter = level.add(order_id, qty);
        orders_by_id_.emplace(order_id, order_iter);
    }

    void modify(OrderID const & order_id, Qty qty, Price price, Level::Set & levels)
    {
        auto iter = orders_by_id_.find(order_id);
        if (iter == orders_by_id_.end())
        {
            // Should not happen since implies modifying an order never added.
            IF_DEBUG(
                std::cerr << "Unable to modify unknown order: " << order_id << std::endl;
                //assert(false);
            )
            return; // Ignoring for now.
        }

        // Get order and level it's contained within.
        // Note: A Level holds a pointer to the container it is within, so if level.levels_ != &levels,
        // then the side was modified (e.g., &buy_levels_ != &sell_levels_).
        auto && order_iter = iter->second;
        auto && order = *order_iter;
        assert(order.level_);
        auto && level = *order.level_;
        assert(level.levels_);
        if (level.levels_ == &levels and level.price() == price)
        {
            if (qty == order.qty())
            {
                return; // No change.
            }

            // Modify qty such that order loses queue position.
            // Should order lose position if new qty is less than original qty?
            level.modify(order, qty);
        }
        else // New side or price
        {
//#define USE_CANCEL_ADD_FOR_MODIFY
#ifdef USE_CANCEL_ADD_FOR_MODIFY
            // If modifying the side or price, we effectively have a new order,
            // so cancel old order and add new order.
            cancel(order_id);
            add(order_id, qty, price, levels);
#else
            // Transfer order to new price level using list::splice() since it does not invalidate iterators.
            // This should be more efficient than cancel-add since the node is not reallocated.
            auto new_levels = level.levels_;
            if (level.levels_ != &levels)
            {
                new_levels = &levels;
            }

            // Search for level with given price, inserting it if it does not exist using hint.
            auto level_iter = new_levels->lower_bound(price);
            if (level_iter == new_levels->end() or level_iter->price() != price)
            {
                level_iter = new_levels->emplace_hint(
                      level_iter
                    , price
                    );
            }

            // Transfer order to end of new level, updating order and level internals.
            Level & new_level = const_cast<Level &>(*level_iter);
            new_level.levels_ = new_levels;
            new_level.iterator_ = level_iter;
            auto & new_orders = new_level.orders_;
            new_orders.splice(new_orders.end(), level.orders_, order_iter);
            order.level_ = &new_level;
            //order.iterator_ = --new_orders.end(); // Not required since not invalidated by splice.
            new_level.qty_ += qty;

            // Update old level, removing it if empty. Finally, set new order qty.
            if (level.empty())
            {
                level.levels_->erase(level.iterator_);
            }
            else
            {
                level.qty_ -= order.qty();
            }
            order.qty_ = qty;
#endif // USE_CANCEL_ADD_FOR_MODIFY
        }
    }

    // Match order with orders in this level set.
    // The book itself is not modified, only the list of trades is generated.
    // The comparison function returns true if the order price matches the level price.
    template <typename Iterator, typename MatchPredicate>
    Qty match(
          Side side
        , OrderID const & order_id
        , Qty qty
        , Price price
        , Iterator levels_begin
        , Iterator levels_end
        , Trades & trades
        , MatchPredicate const & match_predicate
        )
    {
        Qty leaves_qty = qty;
        for (Iterator iter = levels_begin; iter != levels_end; ++iter)
        {
            auto && level = *iter;
            if (not match_predicate(price, level.price()))
            {
                break;
            }

            for (auto && order : level.orders())
            {
                // Prevent self-match.
                // For example, if an order's side is modified, we do not want to match with itself
                // if the pre-modified order is still in the book.
                if (order_id == order.order_id())
                {
                    continue;
                }

                Qty const matched_qty = std::min(leaves_qty, order.qty());

                // Save both passive and aggressive orders that matched.
                // Save a copy of the passive order so we can access its level.
                trades.emplace_back(Trade{
                      level.price()
                    , order
                    , price
                    , Order{order_id, matched_qty}
                    });

                leaves_qty -= matched_qty;
                if (leaves_qty.is_zero())
                {
                    return leaves_qty;
                }
            }
        }
        return leaves_qty;
    }

    // Cancel fully filled orders and modify qty of partially filled orders.
    void fill_orders(Trades & trades)
    {
        for (auto && trade : trades)
        {
            auto leaves_qty = trade.passive_order.qty() - trade.aggressive_order.qty();
            if (leaves_qty.is_zero())
            {
                cancel(trade.passive_order.order_id());
            }
            else
            {
                // Note: pass *iterator_ as the order to modify the level since passive_order itself is only a copy,
                // not the order actually in the level.
                auto && level = trade.passive_order.level_;
                auto && order = *trade.passive_order.iterator_;
                level->modify_qty(order, leaves_qty);

                // Require the passive order's qty to always be equal to the aggressive order's qty for output.
                trade.passive_order.qty(trade.aggressive_order.qty());
            }
        }
    }

private:
    Level::Set buy_levels_;
    Level::Set sell_levels_;

    // Maps order ID directly to its location in a level.
    using OrdersByID = std::unordered_map<OrderID, Order::Queue::iterator>;
    OrdersByID orders_by_id_;
};

std::ostream & operator<<(std::ostream & os, Book const & book)
{
    os << "SELL:\n";
    for (auto && level : book.sell_levels())
    {
        os << level << '\n';
    }

    os << "BUY:\n";
    for (auto && level : book.buy_levels())
    {
        os << level << '\n';
    }

    return os.flush();
}

using BookPtr = std::shared_ptr<Book>;


/*
 * 4. Matching Engine - Matchine engine dispatches events to the order book and handles trade events.
 */

class MatchingEngine
{
public:
    MatchingEngine(BookPtr book)
        : book_{std::move(book)}
    {
        trades_.reserve(1024);
    }

    BookPtr const & book() { return book_; }
    Trades const & trades() const { return trades_; }

    void handle(BuyOrder const & msg)
    {
        handle_add(Side::Buy, msg);
    }

    void handle(SellOrder const & msg)
    {
        handle_add(Side::Sell, msg);
    }

    // Handle buys and sells the same by just passing in the side as a parameter.
    template <typename AddOrder_T>
    void handle_add(Side side, AddOrder_T const & msg)
    {
        trades_.clear();
        Qty const leaves_qty = book_->match(side, msg.order_id, msg.qty, msg.price, trades_);
        if (leaves_qty.is_zero())
        {
            // Aggressive order is fully filled, so done.
            return;
        }

        switch (msg.tif)
        {
            case TIF::GFD:
            {
                book_->add(side, msg.order_id, leaves_qty, msg.price);
                break;
            }

            case TIF::IOC:
            {
                // Do not add order to the book regardless of leaves_qty.
                break;
            }

            case TIF::Invalid:
            {
                break;
            }
        }
    }

    void handle(CancelOrder const & msg)
    {
        book_->cancel(msg.order_id);
    }

    void handle(ModifyOrder const & msg)
    {
        // A modify may match if its price or side changed.
        trades_.clear();
        Qty const leaves_qty = book_->match(msg.side, msg.order_id, msg.qty, msg.price, trades_);
        if (leaves_qty.is_zero())
        {
            // Order is fully filled, but we must still cancel the original order since this is a modify.
            book_->cancel(msg.order_id);
            return;
        }

        // Modify the book (use leaves_qty in case of matching).
        book_->modify(msg.side, msg.order_id, leaves_qty, msg.price);
    }

    void handle(ClearBook const &)
    {
        book_->clear();
    }

private:
    BookPtr book_;
    Trades trades_;
};

using MatchingEnginePtr = std::shared_ptr<MatchingEngine>;


/*
 * 5. Command Processor - Reads and dispatches commands to the matching engine.
 * The command processor handles I/O by reading and parsing commands from an input stream,
 * normalizing and converting the lines into messages acceptable by the matching engine,
 * handling the messages with the matching engine,
 * and writing any results to an output stream.
 */

// CommandProcessor_T handles all input processing and uses CRTP to statically handle messages in derived class.
// Derived_T must implement handlers for each message type:
// void handle(BuyOrder)
// void handle(SellOrder)
// void handle(CancelOrder)
// void handle(ModifyOrder)
// void handle(PrintBook)
// void handle(ClearBook)

template <typename Derived_T>
class CommandProcessor_T
{
public:
    CommandProcessor_T()
    {
        init_cmd_handlers();
    }

    void run(std::istream & is)
    {
        std::string cmd{};
        while (is >> cmd)
        {
            try
            {
                handle(cmd, is);
            }
            catch (std::exception const & error)
            {
                IF_DEBUG(std::cerr << error.what() << std::endl;)
            }
        }
    }

protected:
    void init_cmd_handlers()
    {
        // Initialize mapping from command to handler function.
        using std::placeholders::_1;
        cmd_to_handler_["BUY"] = std::bind(&CommandProcessor_T::handle<BuyOrder>, this, _1);
        cmd_to_handler_["SELL"] = std::bind(&CommandProcessor_T::handle<SellOrder>, this, _1);
        cmd_to_handler_["CANCEL"] = std::bind(&CommandProcessor_T::handle<CancelOrder>, this, _1);
        cmd_to_handler_["MODIFY"] = std::bind(&CommandProcessor_T::handle<ModifyOrder>, this, _1);
        cmd_to_handler_["PRINT"] = std::bind(&CommandProcessor_T::handle<PrintBook>, this, _1);
        cmd_to_handler_["CLEAR"] = std::bind(&CommandProcessor_T::handle<ClearBook>, this, _1);
    }

    void handle(std::string const & cmd, std::istream & is)
    {
        // Lookup and call handler for the command.
        assert(not cmd.empty());
        auto iter = cmd_to_handler_.find(cmd);
        if (iter == cmd_to_handler_.end())
        {
            std::stringstream msg{};
            msg << "Unknown command " << cmd;
            throw std::invalid_argument{msg.str()};
        }
        auto && handler = iter->second;
        handler(is);
    }

    template <typename Msg_T>
    void handle(std::istream & is)
    {
        // Load each message type with input stream and dispatch it statically.
        Msg_T msg{};
        is >> msg;
        if (msg.is_invalid())
        {
            throw std::invalid_argument{"Skipping invalid message"};
        }
        static_cast<Derived_T *>(this)->Derived_T::handle(msg);
    }

private:
    using Handler = std::function<void (std::istream &)>;
    using CmdToHandler = std::unordered_map<std::string, Handler>;
    CmdToHandler cmd_to_handler_;
};


class CommandProcessor
    : public CommandProcessor_T<CommandProcessor>
{
public:
    CommandProcessor(MatchingEnginePtr matching_engine, std::ostream & os)
        : CommandProcessor_T<CommandProcessor>()
        , matching_engine_{std::move(matching_engine)}
        , os_{os}
    {
    }

    void handle(BuyOrder const & msg)
    {
        matching_engine_->handle(msg);
        os_ << matching_engine_->trades();
    }

    void handle(SellOrder const & msg)
    {
        matching_engine_->handle(msg);
        os_ << matching_engine_->trades();
    }

    void handle(CancelOrder const & msg)
    {
        matching_engine_->handle(msg);
    }

    void handle(ModifyOrder const & msg)
    {
        matching_engine_->handle(msg);
        os_ << matching_engine_->trades();
    }

    void handle(PrintBook const &)
    {
        os_ << *matching_engine_->book();
    }

    void handle(ClearBook const & msg)
    {
        matching_engine_->handle(msg);
    }

private:
    MatchingEnginePtr matching_engine_;
    std::ostream & os_;
};


// Command processor that simply writes normalized messages.
class CommandWriter
    : public CommandProcessor_T<CommandWriter>
{
public:
    CommandWriter(std::ostream & os)
        : CommandProcessor_T<CommandWriter>()
        , os_{os}
    {
    }

    // Handle any message type (BuyOrder, CancelOrder, etc.) the same,
    // just write to output stream.
    template <typename Msg_T>
    void handle(Msg_T const & msg)
    {
        os_ << msg << std::endl;
    }

private:
    std::ostream & os_;
};


template <typename T>
class ThreadsafeQueue
{
public:
    using container_type = std::deque<T>;
    ThreadsafeQueue() = default;

    void push(T value)
    {
        std::unique_lock<decltype(mutex_)> lock{mutex_};
        queue_.push_back(std::move(value));
        lock.unlock();
        cond_var_.notify_one();
    }

    void wait_and_pop(T & value)
    {
        std::unique_lock<decltype(mutex_)> lock{mutex_};
        cond_var_.wait(lock,
            [this]()
            {
                // TODO: need a done flag that producer can set so consumer can stop waiting when done too
                //return not queue_.empty() or done; // must then check if empty/done when wait() returns though
                return not queue_.empty();
            });
        value = queue_.front();
        queue_.pop_front();
    }

    bool try_pop(T & value)
    {
        std::lock_guard<decltype(mutex_)> lock{mutex_};
        if (queue_.empty())
        {
            return false;
        }
        value = queue_.front();
        queue_.pop_front();
        return true;
    }

    bool empty() const
    {
        std::lock_guard<decltype(mutex_)> lock{mutex_};
        return queue_.empty();
    }

private:
    container_type queue_;
    mutable std::mutex mutex_;
    std::condition_variable cond_var_;
};

using Task = std::function<void ()>;
using TaskQueue = ThreadsafeQueue<Task>;
using TaskQueuePtr = std::shared_ptr<TaskQueue>;

// Queues commands in a task queue.
class QueueingCommandProcessor
    : public CommandProcessor_T<QueueingCommandProcessor>
{
public:
    QueueingCommandProcessor(TaskQueuePtr task_queue, MatchingEnginePtr matching_engine, std::ostream & os)
        : CommandProcessor_T<QueueingCommandProcessor>()
        , task_queue_{std::move(task_queue)}
        , matching_engine_{std::move(matching_engine)}
        , os_{os}
    {
    }

    void handle(BuyOrder const & msg)
    {
        task_queue_->push(
            [this, msg=std::move(msg)]()
            {
                matching_engine_->handle(msg);
                os_ << matching_engine_->trades();
            });
    }

    void handle(SellOrder const & msg)
    {
        task_queue_->push(
            [this, msg=std::move(msg)]()
            {
                matching_engine_->handle(msg);
                os_ << matching_engine_->trades();
            });
    }

    void handle(CancelOrder const & msg)
    {
        task_queue_->push(
            [this, msg=std::move(msg)]()
            {
                matching_engine_->handle(msg);
            });
    }

    void handle(ModifyOrder const & msg)
    {
        task_queue_->push(
            [this, msg=std::move(msg)]()
            {
                matching_engine_->handle(msg);
                os_ << matching_engine_->trades();
            });
    }

    void handle(PrintBook const &)
    {
        task_queue_->push(
            [this]()
            {
                os_ << *matching_engine_->book();
            });
    }

    void handle(ClearBook const & msg)
    {
        task_queue_->push(
            [this, msg=std::move(msg)]()
            {
                matching_engine_->handle(msg);
            });
    }

private:
    TaskQueuePtr task_queue_;
    MatchingEnginePtr matching_engine_;
    std::ostream & os_;
};


/*
 * 6. Main - Make and run the command processor with a matching engine using stdin and stdout streams.
 */

// Disable synchronization between the C and C++ standard streams for faster I/O.
static const int __ = []() {
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);
    std::cout.tie(nullptr);
    return 0;
}();

namespace {
void run_test(MatchingEnginePtr const & matching_engine) __attribute__((unused));
void run_all_tests();
}

int
main(int argc, char * argv[])
{
    if (argc > 1 and std::string{argv[1]} == "--run-tests")
    {
        run_all_tests();
        return EXIT_SUCCESS;
    }

    auto book = std::make_shared<Book>();
    auto matching_engine = std::make_shared<MatchingEngine>(book);
    if (argc > 1 and std::string{argv[1]} == "--run-threads")
    {
        // Run with multiple threads.
        auto task_queue = std::make_shared<TaskQueue>();
        std::atomic<bool> is_producer_done{false};
        QueueingCommandProcessor cmd_processor{task_queue, matching_engine, std::cout};
        std::thread producer{
            [&cmd_processor, &is_producer_done]()
            {
                cmd_processor.run(std::cin);
                is_producer_done.store(true);
            }};

        std::thread consumer{
            [task_queue, &is_producer_done]()
            {
                Task task{};

                // Execute tasks while producer is running.
                while (not is_producer_done)
                {
                    task_queue->wait_and_pop(task);
                    task();
                }

                // Execute remaining tasks after producer is done.
                while (task_queue->try_pop(task))
                {
                    task();
                }
            }};

        producer.join();
        consumer.join();
    }
    else
    {
        // Single threaded.
        CommandProcessor cmd_processor{matching_engine, std::cout};
        //CommandWriter cmd_processor{std::cout};
        cmd_processor.run(std::cin);
        //run_test(matching_engine);
    }

    return EXIT_SUCCESS;
}


/*
 * 7. Unit Tests - Tests matching engine with various inputs.
 */

namespace {

void run_test(MatchingEnginePtr const & matching_engine)
{
    // Auto-generate orders to test the matching engine.
    std::uint64_t order_id = 0;
    for (std::uint64_t i = 0; i != 10; ++i)
    {
        auto msg = BuyOrder{TIF::GFD, Price{100 * i + 100}, Qty{i + 1}, OrderID{"order_" + std::to_string(order_id++)}};
        std::cout << msg << std::endl;
        matching_engine->handle(msg);
        std::cout << matching_engine->trades();
    }
    std::cout << *matching_engine->book() << std::endl;

    for (std::uint64_t i = 0; i != 10; ++i)
    {
        auto msg = SellOrder{TIF::GFD, Price{100 * i + 100}, Qty{i + 1}, OrderID{"order_" + std::to_string(order_id++)}};
        std::cout << msg << std::endl;
        matching_engine->handle(msg);
        std::cout << matching_engine->trades();
    }
    std::cout << *matching_engine->book() << std::endl;

    {
        auto msg = ModifyOrder{OrderID{"order_19"}, Side::Buy, Price{700}, Qty{18}};
        std::cout << msg << std::endl;
        matching_engine->handle(msg);
        std::cout << *matching_engine->book() << std::endl;
        std::cout << matching_engine->trades();
    }
    {
        //auto msg = ModifyOrder{OrderID{"order_19"}, Side::Sell, Price{100}, Qty{37}};
        //auto msg = ModifyOrder{OrderID{"order_19"}, Side::Buy, Price{1000}, Qty{18}};
        auto msg = ModifyOrder{OrderID{"order_19"}, Side::Sell, Price{100}, Qty{18}};
        std::cout << msg << std::endl;
        matching_engine->handle(msg);
        std::cout << *matching_engine->book() << std::endl;
        std::cout << matching_engine->trades();
    }
}

bool run_test_1();
bool run_test_2();
bool run_test_3();
bool run_test_4();
bool run_test_5();
bool run_test_6();
bool run_test_7();
bool run_test_8();
bool run_test_9();
bool run_test_10();
bool run_test_11();
bool run_test_12();
bool run_test_13();
bool run_test_14();
bool run_test_15();
bool run_test_16();
bool run_test_17();
bool run_test_18();
bool run_test_19();
bool run_test_20();
bool run_test_21();
bool run_test_22();

void run_all_tests()
{
    run_test_1();
    run_test_2();
    run_test_3();
    run_test_4();
    run_test_5();
    run_test_6();
    run_test_7();
    run_test_8();
    run_test_9();
    run_test_10();
    run_test_11();
    run_test_12();
    run_test_13();
    run_test_14();
    run_test_15();
    run_test_16();
    run_test_17();
    run_test_18();
    run_test_19();
    run_test_20();
    run_test_21();
    run_test_22();
}

bool run_test(std::string const & test_name, std::string const & input, std::string const & expected_output)
{
    std::stringstream is{};
    is << input;

    std::stringstream os{};
    auto book = std::make_shared<Book>();
    auto matching_engine = std::make_shared<MatchingEngine>(book);
    CommandProcessor cmd_processor{matching_engine, os};
    cmd_processor.run(is);

    if (os.str() == expected_output)
    {
        std::cout << "OK: " << test_name << std::endl;
        return true;
    }

    std::cout << "FAIL: " << test_name << std::endl;
    std::cout << "Input:" << std::endl;
    std::cout << is.str() << std::endl;
    std::cout << "Expected:" << std::endl;
    std::cout << expected_output << std::endl;
    std::cout << "Actual:" << std::endl;
    std::cout << os.str() << std::endl;
    return false;
}

bool run_test_1()
{
    return run_test("Example 1",
R"raw(BUY GFD 1000 10 order1
PRINT
)raw",
R"raw(SELL:
BUY:
1000 10
)raw");
}

bool run_test_2()
{
    return run_test("Example 2",
R"raw(BUY GFD 1000 10 order1
BUY GFD 1000 20 order2
PRINT
)raw",
R"raw(SELL:
BUY:
1000 30
)raw");
}

bool run_test_3()
{
    return run_test("Example 3",
R"raw(BUY GFD 1000 10 order1
BUY GFD 1001 20 order2
PRINT
)raw",
R"raw(SELL:
BUY:
1001 20
1000 10
)raw");
}

bool run_test_4()
{
    return run_test("Example 4",
R"raw(BUY GFD 1000 10 order1
SELL GFD 900 20 order2
PRINT
)raw",
R"raw(TRADE order1 1000 10 order2 900 10
SELL:
900 10
BUY:
)raw");
}

bool run_test_5()
{
    return run_test("Example 5",
R"raw(BUY GFD 1000 10 order1
BUY GFD 1010 10 order2
SELL GFD 1000 15 order3
)raw",
R"raw(TRADE order2 1010 10 order3 1000 10
TRADE order1 1000 5 order3 1000 5
)raw");
}

bool run_test_6()
{
    return run_test("Modify",
R"raw(BUY GFD 1000 10 order1
BUY GFD 1000 10 order2
MODIFY order1 BUY 1000 20
SELL GFD 900 20 order3
)raw",
R"raw(TRADE order2 1000 10 order3 900 10
TRADE order1 1000 10 order3 900 10
)raw");
}


bool run_test_7()
{
    return run_test("Multiple orders",
R"raw(BUY GFD 1000 10 order1
BUY GFD 1000 15 order2
BUY GFD 900 20 order3
BUY GFD 800 15 order4
SELL GFD 1100 30 order5
SELL GFD 1200 50 order6
SELL GFD 1200 70 order7
SELL GFD 1300 60 order8
PRINT
BUY GFD 1200 160 order9
PRINT
)raw",
R"raw(SELL:
1300 60
1200 120
1100 30
BUY:
1000 25
900 20
800 15
TRADE order5 1100 30 order9 1200 30
TRADE order6 1200 50 order9 1200 50
TRADE order7 1200 70 order9 1200 70
SELL:
1300 60
BUY:
1200 10
1000 25
900 20
800 15
)raw");
}

bool run_test_8()
{
    return run_test("Trade 1 - 2 orders at same price, full fill",
R"raw(BUY GFD 1000 10 order1
BUY GFD 1000 10 order2
MODIFY order1 BUY 1000 20
SELL GFD 900 20 order3
BUY GFD 1000 10 order1
PRINT
BUY GFD 1000 10 order2
PRINT
SELL GFD 900 20 order3
PRINT
)raw",
R"raw(TRADE order2 1000 10 order3 900 10
TRADE order1 1000 10 order3 900 10
SELL:
BUY:
1000 10
SELL:
BUY:
1000 20
TRADE order1 1000 10 order3 900 10
TRADE order2 1000 10 order3 900 10
SELL:
BUY:
)raw");
}

bool run_test_9()
{
    return run_test("Trade 2 - 2 orders at different prices, partial fill",
R"raw(BUY GFD 1000 10 order1
BUY GFD 1010 10 order2
SELL GFD 1000 15 order3
PRINT
)raw",
R"raw(TRADE order2 1010 10 order3 1000 10
TRADE order1 1000 5 order3 1000 5
SELL:
BUY:
1000 5
)raw");
}


bool run_test_10()
{
    return run_test("Self-match prevention test - should fully fill order2 and cancel original order1",
R"raw(BUY GFD 1000 10 order1
BUY GFD 1000 10 order2
MODIFY order1 SELL 1000 10
PRINT
)raw",
R"raw(TRADE order2 1000 10 order1 1000 10
SELL:
BUY:
)raw");
}


bool run_test_11()
{
    return run_test("Self-match prevention test - should partially fill order2 and modify original order1",
R"raw(BUY GFD 1000 10 order1
BUY GFD 1000 5 order2
MODIFY order1 SELL 900 10
PRINT
)raw",
R"raw(TRADE order2 1000 5 order1 900 5
SELL:
900 5
BUY:
)raw");
}


bool run_test_12()
{
    return run_test("IOC - book is empty so no matching and orders are not added to book",
R"raw(BUY IOC 1000 10 order1
SELL IOC 1000 10 order2
PRINT
)raw",
R"raw(SELL:
BUY:
)raw");
}


bool run_test_13()
{
    return run_test("IOC - full fill, no orders left",
R"raw(BUY GFD 1000 10 order1
SELL IOC 1000 10 order2
PRINT
)raw",
R"raw(TRADE order1 1000 10 order2 1000 10
SELL:
BUY:
)raw");
}

bool run_test_14()
{
    return run_test("IOC - full fill, order left",
R"raw(BUY GFD 1000 15 order1
SELL IOC 1000 10 order2
PRINT
)raw",
R"raw(TRADE order1 1000 10 order2 1000 10
SELL:
BUY:
1000 5
)raw");
}

bool run_test_15()
{
    return run_test("IOC - partial fill, non-IOC order is left",
R"raw(BUY GFD 900 5 order1
BUY GFD 1000 5 order2
SELL IOC 1000 10 order3
PRINT
)raw",
R"raw(TRADE order2 1000 5 order3 1000 5
SELL:
BUY:
900 5
)raw");
}

bool run_test_16()
{
    return run_test("IOC - full fill over multiple orders, non-IOC order is left",
R"raw(BUY GFD 900 5 order1
BUY GFD 1000 5 order2
BUY GFD 1100 5 order3
SELL IOC 1000 10 order4
PRINT
)raw",
R"raw(TRADE order3 1100 5 order4 1000 5
TRADE order2 1000 5 order4 1000 5
SELL:
BUY:
900 5
)raw");
}

bool run_test_17()
{
    return run_test("Duplicate order id when adding",
R"raw(BUY GFD 900 5 order1
BUY GFD 900 5 order1
PRINT
)raw",
R"raw(SELL:
BUY:
900 5
)raw");
}


bool run_test_18()
{
    return run_test("Unknown order id when cancelling",
R"raw(CANCEL unknown
PRINT
)raw",
R"raw(SELL:
BUY:
)raw");
}

bool run_test_19()
{
    return run_test("Unknown order id when modifying",
R"raw(MODIFY unknown BUY 1000 20
PRINT
)raw",
R"raw(SELL:
BUY:
)raw");
}


bool run_test_20()
{
    return run_test("Invalid price and qty",
R"raw(BUY GFD a 5 order1
BUY GFD 900 b order1
PRINT
)raw",
R"raw()raw");
}


bool run_test_21()
{
    return run_test("Trade - 2 sell orders at same price, full fill",
R"raw(SELL GFD 1000 10 order1
PRINT
SELL GFD 1000 10 order2
PRINT
BUY GFD 1100 20 order3
PRINT
)raw",
R"raw(SELL:
1000 10
BUY:
SELL:
1000 20
BUY:
TRADE order1 1000 10 order3 1100 10
TRADE order2 1000 10 order3 1100 10
SELL:
BUY:
)raw");
}

bool run_test_22()
{
    return run_test("Modify queue position - should partially fill order1 since back of the queue after modify",
R"raw(BUY GFD 1000 10 order1
BUY GFD 1000 10 order2
MODIFY order1 BUY 1000 10
SELL GFD 1000 15 order3
PRINT
)raw",
R"raw(TRADE order1 1000 10 order3 1000 10
TRADE order2 1000 5 order3 1000 5
SELL:
BUY:
1000 5
)raw");
}

}

