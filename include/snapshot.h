#ifndef PRISM_SNAPSHOT_H
#define PRISM_SNAPSHOT_H

#include "dbformat.h"

#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>

namespace prism
{
	class DBImpl;
	class SnapshotRegistry;
	struct SnapshotState;

	class SnapshotRegistry
	{
	public:
		struct Node
		{
			explicit Node(SequenceNumber sequence_number)
			    : sequence(sequence_number)
			{
			}

			const SequenceNumber sequence;

		private:
			friend class SnapshotRegistry;

			Node* prev = nullptr;
			Node* next = nullptr;
			bool linked = false;
		};

		SnapshotRegistry();
		SnapshotRegistry(const SnapshotRegistry&) = delete;
		SnapshotRegistry& operator=(const SnapshotRegistry&) = delete;

		std::shared_ptr<Node> Register(SequenceNumber sequence);
		void Release(Node* node);
		std::optional<SequenceNumber> OldestSequence() const;
		bool Empty() const;
		size_t Size() const;

	private:
		mutable std::mutex mutex_;
		Node head_{ 0 };
		size_t size_ = 0;
	};

	struct SnapshotState
	{
		SnapshotState(SequenceNumber sequence_number, std::shared_ptr<SnapshotRegistry> registry_value,
		    std::shared_ptr<SnapshotRegistry::Node> node_value);
		~SnapshotState();

		const SequenceNumber sequence;
		const std::shared_ptr<SnapshotRegistry> registry;
		const std::shared_ptr<SnapshotRegistry::Node> node;
	};

	class Snapshot
	{
	public:
		Snapshot() = default;
		Snapshot(const Snapshot&) = default;
		Snapshot(Snapshot&&) noexcept = default;
		Snapshot& operator=(const Snapshot&) = default;
		Snapshot& operator=(Snapshot&&) noexcept = default;
		~Snapshot() = default;

	private:
		friend class DBImpl;

		explicit Snapshot(std::shared_ptr<const SnapshotState> state)
		    : state_(std::move(state))
		{
		}

		std::shared_ptr<const SnapshotState> state_;
	};

} // namespace prism

#endif // PRISM_SNAPSHOT_H
