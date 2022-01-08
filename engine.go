package kvs

// KvsEngine interface for a key value storage engine.
type KvsEngine interface {
	// Set Sets the value of a string key to a string.
	//
	// If the key already exists, the previous value will be overwritten.
	Set(key string, value string) error

	// Get Gets the string value of a given string key.
	//
	// Returns `None` if the given key does not exist.
	Get(key string) (string, error)

	// Remove Removes a given key.
	//
	// # Errors
	//
	// It returns `KvsError::KeyNotFound` if the given key is not found.
	Remove(key string) error

	// Clone clone itself
	clone() KvsEngine

	// Shutdown shutdown engine
	shutdown() error
}
