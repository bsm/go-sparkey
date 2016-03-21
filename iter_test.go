package sparkey

import (
	"bytes"
	"io"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("LogIter", func() {
	var subject *LogIter
	var reader *LogReader
	var kv = func() string {
		k, _ := subject.Key()
		v, _ := subject.Value()
		return string(k) + ":" + string(v)
	}

	BeforeEach(func() {
		fname, err := writeDefaultTestHash()
		Expect(err).NotTo(HaveOccurred())
		reader, err = OpenLogReader(fname + ".spl")
		Expect(err).NotTo(HaveOccurred())
		subject, err = reader.Iterator()
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		subject.Close()
		reader.Close()
	})

	It("should open iterators", func() {
		Expect(subject.iter).NotTo(BeNil())
		Expect(subject.log).NotTo(BeNil())
		Expect(subject.State()).To(Equal(ITERATOR_NEW))
		Expect(subject.Valid()).To(BeFalse())
		Expect(subject.EntryType()).To(Equal(ENTRY_DELETE))
		Expect(subject.KeyLen()).To(Equal(uint64(0)))
		Expect(subject.ValueLen()).To(Equal(uint64(0)))
		subject.Close()
	})

	It("should retrieve keys", func() {
		_, err := subject.Key()
		Expect(err).To(Equal(ERROR_LOG_ITERATOR_INACTIVE))

		Expect(subject.Next()).NotTo(HaveOccurred())
		Expect(subject.State()).To(Equal(ITERATOR_ACTIVE))
		Expect(subject.Valid()).To(Equal(true))

		key, err := subject.Key()
		Expect(err).NotTo(HaveOccurred())
		Expect(string(key)).To(Equal("xk"))

		key, err = subject.Key()
		Expect(err).NotTo(HaveOccurred())
		Expect(string(key)).To(Equal(""))

		Expect(subject.Next()).NotTo(HaveOccurred())

		key, err = subject.Key()
		Expect(err).NotTo(HaveOccurred())
		Expect(string(key)).To(Equal("yk"))
	})

	It("should retrieve keys using the io.Reader interface", func() {
		b := make([]byte, 5)
		r := subject.KeyReader()
		n, err := r.Read(b)
		Expect(n).To(Equal(0))
		Expect(err).To(Equal(ERROR_LOG_ITERATOR_INACTIVE))

		Expect(subject.Next()).NotTo(HaveOccurred())
		Expect(subject.State()).To(Equal(ITERATOR_ACTIVE))
		Expect(subject.Valid()).To(Equal(true))

		b = make([]byte, 2)
		n, err = r.Read(b)
		Expect(n).To(Equal(2))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b[:2])).To(Equal("xk"))

		b = make([]byte, 5)
		n, err = r.Read(b)
		Expect(n).To(Equal(0))
		Expect(err).To(Equal(io.EOF))

		Expect(subject.Next()).NotTo(HaveOccurred())

		b = []byte{}
		n, err = r.Read(b)
		Expect(n).To(Equal(0))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(""))

		b = make([]byte, 5)
		n, err = r.Read(b)
		Expect(n).To(Equal(2))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b[:2])).To(Equal("yk"))
	})

	It("should retrieve keys using the io.WriterTo interface", func() {
		buf := new(bytes.Buffer)
		wt := subject.KeyReader()

		buf.Reset()
		n, err := wt.WriteTo(buf)
		Expect(n).To(Equal(int64(0)))
		Expect(err).To(Equal(ERROR_LOG_ITERATOR_INACTIVE))

		Expect(subject.Next()).NotTo(HaveOccurred())
		Expect(subject.State()).To(Equal(ITERATOR_ACTIVE))
		Expect(subject.Valid()).To(Equal(true))

		buf.Reset()
		n, err = wt.WriteTo(buf)
		Expect(n).To(Equal(int64(2)))
		Expect(err).NotTo(HaveOccurred())
		Expect(buf.String()).To(Equal("xk"))

		buf.Reset()
		n, err = wt.WriteTo(buf)
		Expect(n).To(Equal(int64(0)))
		Expect(err).NotTo(HaveOccurred())
		Expect(buf.String()).To(Equal(""))
	})

	It("should retrieve values", func() {
		_, err := subject.Value()
		Expect(err).To(Equal(ERROR_LOG_ITERATOR_INACTIVE))

		Expect(subject.Next()).NotTo(HaveOccurred())
		Expect(subject.State()).To(Equal(ITERATOR_ACTIVE))
		Expect(subject.Valid()).To(Equal(true))

		val, err := subject.Value()
		Expect(err).NotTo(HaveOccurred())
		Expect(string(val)).To(Equal("short"))

		val, err = subject.Value()
		Expect(err).NotTo(HaveOccurred())
		Expect(string(val)).To(Equal(""))

		Expect(subject.Next()).NotTo(HaveOccurred())

		val, err = subject.Value()
		Expect(err).NotTo(HaveOccurred())
		Expect(string(val)).To(Equal("longvalue"))

		Expect(subject.Next()).NotTo(HaveOccurred())

		val, err = subject.Value()
		Expect(err).NotTo(HaveOccurred())
		Expect(string(val)).To(Equal(veryLongString))
	})

	It("should retrieve values using the io.Reader interface", func() {
		b := make([]byte, 5)
		r := subject.ValueReader()
		n, err := r.Read(b)
		Expect(n).To(Equal(0))
		Expect(err).To(Equal(ERROR_LOG_ITERATOR_INACTIVE))

		Expect(subject.Next()).NotTo(HaveOccurred())
		Expect(subject.State()).To(Equal(ITERATOR_ACTIVE))
		Expect(subject.Valid()).To(Equal(true))

		b = make([]byte, 5)
		n, err = r.Read(b)
		Expect(n).To(Equal(5))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal("short"))

		b = make([]byte, 5)
		n, err = r.Read(b)
		Expect(n).To(Equal(0))
		Expect(err).To(Equal(io.EOF))

		Expect(subject.Next()).NotTo(HaveOccurred())

		b = []byte{}
		n, err = r.Read(b)
		Expect(n).To(Equal(0))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(""))

		b = make([]byte, 5)
		n, err = r.Read(b)
		Expect(n).To(Equal(5))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal("longv"))

		Expect(subject.Next()).NotTo(HaveOccurred())

		b = make([]byte, len(veryLongString))
		n, err = io.ReadFull(r, b)
		Expect(n).To(Equal(len(veryLongString)))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(veryLongString))
	})

	It("should retrieve values using the io.WriterTo interface", func() {
		buf := new(bytes.Buffer)
		wt := subject.ValueReader()

		buf.Reset()
		n, err := wt.WriteTo(buf)
		Expect(n).To(Equal(int64(0)))
		Expect(err).To(Equal(ERROR_LOG_ITERATOR_INACTIVE))

		Expect(subject.Next()).NotTo(HaveOccurred())
		Expect(subject.State()).To(Equal(ITERATOR_ACTIVE))
		Expect(subject.Valid()).To(Equal(true))

		buf.Reset()
		n, err = wt.WriteTo(buf)
		Expect(n).To(Equal(int64(5)))
		Expect(err).NotTo(HaveOccurred())
		Expect(buf.String()).To(Equal("short"))

		buf.Reset()
		n, err = wt.WriteTo(buf)
		Expect(n).To(Equal(int64(0)))
		Expect(err).NotTo(HaveOccurred())
		Expect(buf.String()).To(Equal(""))
	})

	It("should navigate", func() {
		// Next
		Expect(subject.Next()).NotTo(HaveOccurred())
		Expect(subject.Valid()).To(BeTrue())
		Expect(subject.State()).To(Equal(ITERATOR_ACTIVE))
		Expect(subject.EntryType()).To(Equal(ENTRY_PUT))
		Expect(kv()).To(Equal("xk:short"))

		// Next
		Expect(subject.Next()).NotTo(HaveOccurred())
		Expect(subject.Valid()).To(BeTrue())
		Expect(subject.State()).To(Equal(ITERATOR_ACTIVE))
		Expect(subject.EntryType()).To(Equal(ENTRY_PUT))
		Expect(kv()).To(Equal("yk:longvalue"))

		// Skip
		Expect(subject.Skip(2)).NotTo(HaveOccurred())
		Expect(subject.Valid()).To(BeTrue())
		Expect(subject.State()).To(Equal(ITERATOR_ACTIVE))
		Expect(subject.EntryType()).To(Equal(ENTRY_DELETE))
		Expect(kv()).To(Equal("yk:"))

		// End-of-iterator
		Expect(subject.Next()).NotTo(HaveOccurred())
		Expect(subject.Valid()).To(BeFalse())
		Expect(subject.State()).To(Equal(ITERATOR_CLOSED))
		Expect(subject.EntryType()).To(Equal(ENTRY_DELETE))
		Expect(kv()).To(Equal(":"))
	})

	It("should reset", func() {
		Expect(subject.Reset()).To(Equal(ERROR_LOG_ITERATOR_INACTIVE))

		Expect(subject.Skip(2)).NotTo(HaveOccurred())
		Expect(kv()).To(Equal("yk:longvalue"))
		Expect(kv()).To(Equal(":"))
		Expect(subject.Reset()).NotTo(HaveOccurred())
		Expect(kv()).To(Equal("yk:longvalue"))
	})

	It("should iterate", func() {
		contents := make([]string, 0, 5)
		for subject.Next(); subject.Valid(); subject.Next() {
			contents = append(contents, kv())
		}
		Expect(contents).To(Equal([]string{
			"xk:short", "yk:longvalue", "zk:" + veryLongString, "yk:",
		}))
		Expect(subject.Err()).NotTo(HaveOccurred())
	})

	It("should compare", func() {
		iter, err := reader.Iterator()
		Expect(err).NotTo(HaveOccurred())
		defer iter.Close()

		_, err = subject.Compare(iter)
		Expect(err).To(Equal(ERROR_LOG_ITERATOR_INACTIVE))

		Expect(subject.Next()).NotTo(HaveOccurred())
		Expect(iter.Next()).NotTo(HaveOccurred())

		val, err := subject.Compare(iter)
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal(0))

		Expect(iter.Next()).NotTo(HaveOccurred())
		val, err = subject.Compare(iter)
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal(-1))

		Expect(iter.Next()).NotTo(HaveOccurred())
		val, err = iter.Compare(subject)
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal(1))
	})
})

var _ = Describe("HashIter", func() {
	var subject *HashIter
	var reader *HashReader
	var kv = func() string {
		k, _ := subject.Key()
		v, _ := subject.Value()
		return string(k) + ":" + string(v)
	}

	BeforeEach(func() {
		fname, err := writeDefaultTestHash()
		Expect(err).NotTo(HaveOccurred())
		reader, err = Open(fname)
		Expect(err).NotTo(HaveOccurred())
		subject, err = reader.Iterator()
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		subject.Close()
		reader.Close()
	})

	It("should iterate over active keys", func() {
		contents := make([]string, 0, 5)
		for subject.NextLive(); subject.Valid(); subject.NextLive() {
			contents = append(contents, kv())
		}
		Expect(contents).To(Equal([]string{
			"xk:short", "zk:" + veryLongString,
		}))
		Expect(subject.Err()).NotTo(HaveOccurred())
	})

	It("should seek keys", func() {
		err := subject.Seek([]byte("missing"))
		Expect(err).NotTo(HaveOccurred())
		Expect(subject.State()).To(Equal(ITERATOR_INVALID))
		err = subject.Seek([]byte("yk"))
		Expect(err).NotTo(HaveOccurred())
		Expect(subject.State()).To(Equal(ITERATOR_INVALID))
		err = subject.Seek([]byte("zk"))
		Expect(err).NotTo(HaveOccurred())
		Expect(subject.State()).To(Equal(ITERATOR_ACTIVE))
		Expect(kv()).To(Equal(":" + veryLongString))
		err = subject.Seek([]byte("xk"))
		Expect(err).NotTo(HaveOccurred())
		Expect(subject.State()).To(Equal(ITERATOR_ACTIVE))
		Expect(kv()).To(Equal(":short"))
	})

	It("should retrieve values", func() {
		// Get missing
		val, err := subject.Get([]byte("missing"))
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeNil())

		// Other missing
		val, err = subject.Get([]byte("x"))
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeNil())

		// Get existing
		val, err = subject.Get([]byte("zk"))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(val)).To(Equal(veryLongString))

		val, err = subject.Get([]byte("xk"))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(val)).To(Equal("short"))

		// Get deleted
		val, err = subject.Get([]byte("yk"))
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeNil())
	})

})
