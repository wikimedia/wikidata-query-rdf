Schemas can be placed here while they are in 'development' / prototyping phase.
While in development, any event data created that conforms to these schemas is
considered ephemeral and can be deleted / lost at any time.  As such,
we don't have to think as hard about making backwards incompatible changes.

CI will still enforce backwards compatibility, but feel free to make new
major versions at will as you develop.  When you move out of this development
namespace, you can start over with brand new schemas at version 1.0.0.

