import crypto from "crypto"

const TileService = (event) => {

    const { Query, GetColumns } = event.dbManager

    // クエリを登録しておく(クエリパラメータがなければviewまで生成する)
    const post_sql = async (params) => {

        const { q, values } = params.data

        // クエリをベースにハッシュキーを生成
        const hash = crypto.createHash("md5").update(q.replace("\n", "").replace(/ +/, ' ')).digest("hex")


        // すでにあった場合
            // materialized viewならrefresh materialized viewを実行
            // viewなら何もしない
            // tableならdrop&create
        // なかった場合
        if (params.data.q.match(/\$[0-9]+/) /** &&  パラメータ数が与えられてるクエリパラメータを超えてる場合 */) {
            // クエリパラメータ付きでハッシュキーを生成してtileConfigにクエリのみ保管
        } else {
            // 定義に従ってview又はmaterialized viewまたはtableを生成し、viewNameに格納
            // 上記で生成したもののクエリをlimit 0で実行し、カラムリストを取得
            // materialized viewの場合はindexiesの記述に従ってindexを作成
        }

        // idを生成して返す

    }

    // クエリを更新する
    const put_sql = async (params) => {

    }

    const get_index = async (params) => {
        // tile/{id}/[...queryParam]/[z]/[x]/[y].ext
        const id = params.path.shift()
        const z = params.path.pop()
        const yExt = params.path.pop()
        const [_, y, ext] = yExt.match(/^(\d+)\.(\w+)$/)
        const x = params.path.pop()

        const tileConfig = await getTileConfig(id) // redisからtileのconfを取得

        // viewNameがなければ、クエリパラメータを付与したビューを作成する
        if (tileConfig.viewName) {
            switch(tileConfig.tableType) {
                case "view":
                    tileConfig.viewName = await CreateTilesetView(`${tileConfig.sql}`, params.path, tileConfig)
                    params.path = null
                    break
                case "mview":
                    tileConfig.viewName = await CreateTilesetMaterializedView(`${tileConfig.sql}`, params.path, tileConfig)
                    params.path = null
                    break
                case "table":
                    tileConfig.viewName = await CreateTilesetTable(`${tileConfig.sql}`, params.path, tileConfig)
                    params.path = null
                    break
                case "query":
                    tileConfig.viewName = tileConfig.sql
                    break
                default:
                    // 未定義の処理エラー
            }
        }

        // TODO: 拡張子によってはmapnikを使ってtile画像を生成する

        // すでにキャッシュが無いかチェック
        let tile = await getTileCache(tileConfig, z, x, y)
        if (!tile) {
            // MVT生成クエリを実行
            tile = await generateTile(tileConfig, z, x, y)
            // タイル自体がデカい場合がってLambdaでは返せないのでs3指定があれば保存
            // 結果を
        }
        return ResponseMVTSync(tile)
    }

    const delete_garbage = (params) => {
        // 条件に従って、古いデータを削除してく(古い方から容量指定、期限切れのデータなど)
    }

    return {
        get_index,
        post_sql,
        put_sql,
        delete_garbage,
    }

}

export default TileService
