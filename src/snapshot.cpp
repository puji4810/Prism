#include "snapshot.h"

#include <utility>

namespace prism
{
	SnapshotRegistry::SnapshotRegistry()
	{
		head_.prev = &head_;
		head_.next = &head_;
		head_.linked = true;
	}

	std::shared_ptr<SnapshotRegistry::Node> SnapshotRegistry::Register(SequenceNumber sequence)
	{
		auto node = std::make_shared<Node>(sequence);
		std::lock_guard<std::mutex> lock(mutex_);
		node->prev = head_.prev;
		node->next = &head_;
		node->prev->next = node.get();
		node->next->prev = node.get();
		node->linked = true;
		++size_;
		return node;
	}

	void SnapshotRegistry::Release(Node* node)
	{
		if (node == nullptr)
		{
			return;
		}

		std::lock_guard<std::mutex> lock(mutex_);
		if (!node->linked)
		{
			return;
		}

		node->prev->next = node->next;
		node->next->prev = node->prev;
		node->prev = nullptr;
		node->next = nullptr;
		node->linked = false;
		--size_;
	}

	std::optional<SequenceNumber> SnapshotRegistry::OldestSequence() const
	{
		std::lock_guard<std::mutex> lock(mutex_);
		if (size_ == 0)
		{
			return std::nullopt;
		}
		return head_.next->sequence;
	}

	bool SnapshotRegistry::Empty() const
	{
		std::lock_guard<std::mutex> lock(mutex_);
		return size_ == 0;
	}

	size_t SnapshotRegistry::Size() const
	{
		std::lock_guard<std::mutex> lock(mutex_);
		return size_;
	}

	SnapshotState::SnapshotState(SequenceNumber sequence_number, std::shared_ptr<SnapshotRegistry> registry_value,
	    std::shared_ptr<SnapshotRegistry::Node> node_value)
	    : sequence(sequence_number)
	    , registry(std::move(registry_value))
	    , node(std::move(node_value))
	{
	}

	SnapshotState::~SnapshotState()
	{
		if (registry)
		{
			registry->Release(node.get());
		}
	}

} // namespace prism
