# baileys guide

The complete reference (all types, classes, options) is [DOCS.md](../DOCS.md).

* [Commands, permissions and middleware](commands.md)
* [Messages, media, conversations and the send queue](messages.md)
* [Events](events.md)
* [Groups, contacts and LID / phone numbers](groups.md)
* [Storage backends](storage.md)
* [Plugins and multiple accounts](plugins.md)
* [Testing, including against real WhatsApp](testing.md)

Everything here is a layer over the 1:1 port of Baileys. The low level socket is always available as `client.sock`
and unknown attributes of `Client` are forwarded to it.
