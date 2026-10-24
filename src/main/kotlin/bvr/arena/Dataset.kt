package bvr.arena

import java.io.File

/** HttpArena's `data/dataset.json`, read with a parser just large enough for it, so no server here needs a JSON library. */
object Dataset {
    class Item(
        val id: Int,
        val name: String,
        val category: String,
        val price: Int,
        val quantity: Int,
        val active: Boolean,
        val tags: List<String>,
        val score: Int,
        val ratingCount: Int,
    )

    fun load(file: File): List<Item> {
        @Suppress("UNCHECKED_CAST")
        val raw = Parser(file.readText()).value() as List<Map<String, Any?>>
        return raw.map { o ->
            val rating = o["rating"] as Map<*, *>
            @Suppress("UNCHECKED_CAST")
            Item(
                id = (o["id"] as Long).toInt(),
                name = plain(o["name"] as String),
                category = plain(o["category"] as String),
                price = (o["price"] as Long).toInt(),
                quantity = (o["quantity"] as Long).toInt(),
                active = o["active"] as Boolean,
                tags = (o["tags"] as List<String>).map(::plain),
                score = (rating["score"] as Long).toInt(),
                ratingCount = (rating["count"] as Long).toInt(),
            )
        }
    }

    /** The servers write these strings without escaping, so each must be printable ASCII with no quote or backslash. */
    private fun plain(s: String): String {
        require(s.all { it in ' '..'~' && it != '"' && it != '\\' }) { "needs escaping: $s" }
        return s
    }

    private class Parser(private val s: String) {
        private var i = 0

        fun value(): Any? {
            skip()
            return when (s[i]) {
                '{' -> obj()
                '[' -> arr()
                '"' -> str()
                't' -> { i += 4; true }
                'f' -> { i += 5; false }
                'n' -> { i += 4; null }
                else -> num()
            }
        }

        private fun obj(): Map<String, Any?> {
            val m = LinkedHashMap<String, Any?>()
            i++
            while (true) {
                skip()
                if (s[i] == '}') { i++; return m }
                if (s[i] == ',') { i++; continue }
                val k = str()
                skip(); i++ // ':'
                m[k] = value()
            }
        }

        private fun arr(): List<Any?> {
            val l = ArrayList<Any?>()
            i++
            while (true) {
                skip()
                if (s[i] == ']') { i++; return l }
                if (s[i] == ',') { i++; continue }
                l += value()
            }
        }

        private fun str(): String {
            val from = ++i
            while (s[i] != '"') {
                require(s[i] != '\\') { "escapes are not supported" }
                i++
            }
            return s.substring(from, i++)
        }

        private fun num(): Long {
            val from = i
            while (i < s.length && (s[i] == '-' || s[i].isDigit())) i++
            return s.substring(from, i).toLong()
        }

        private fun skip() {
            while (s[i].isWhitespace()) i++
        }
    }
}
