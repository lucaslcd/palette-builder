import { inject, Service } from '@angular/core';
import { BitmapDisposition, BitmapFormat, ExportData } from './export.state';
import { PaletteModel } from './color.state';
import { UtilsService } from './utils.service';

@Service()
export class ExportService {
    utils = inject(UtilsService)

    export(paletteData: PaletteModel, exportData: ExportData) {
        switch (exportData.type) {
            case 'bitmap':
                this._exportBitmap(paletteData, exportData)
                break;
            case 'json':
                this._exportJSON(paletteData)
                break;
        }
    }

    private _exportBitmap(paletteData: PaletteModel, exportData: { format: BitmapFormat, disposition: BitmapDisposition }) {
        const xUngroupedArr = new Array<{h:number, s: number}>();
        const ySet = new Set<number>();
        
        //1. create sets for hue, saturation and lights so that we can calculate dimensions
        paletteData.colors.forEach(el=>{
            el.shades.forEach(el2=>{
                const hsl = [el.hue, el.saturation, el2];
                xUngroupedArr.push({h:hsl[0], s: hsl[1]})
                ySet.add(hsl[2])
            })
        })
        const xArr =
            Array.from(
                new Set(
                    xUngroupedArr.map(el=>JSON.stringify(el))
                )
            ).map(el=>JSON.parse(el)) as Array<{h:number, s: number}>;
        const yArr = Array.from(ySet).toSorted((a,b)=>b - a)

        //2. create the canvas
        const canvas = document.createElement('canvas')
        canvas.width = 1024
        canvas.height = 1024
        const ctx = canvas.getContext('2d')

        if (!ctx) {
            return;
        }

        const ctxWidth = canvas.width
        const ctxHeight = canvas.height

        const xSize = xArr.length
        const ySize = ySet.size

        const cellWidth = Math.round(ctxWidth / xSize)
        const cellHeight = Math.round(ctxHeight / ySize)

        xArr
        .toSorted((a,b)=>{
            if (exportData.disposition === 'sat-hua.lig') {
                return a.h % 360 - b.h % 360
            }
            else {
                return Math.abs(a.h % 360 - 180) - Math.abs(b.h % 360 - 180)
            }
        })
        .toSorted((a,b)=>a.s-b.s)
        .forEach(({h,s}, x)=>{
            yArr.forEach((l, y)=>{
                ctx.fillStyle = `hsl(${h}, ${s * 100}%, ${l * 100}%)`
                console.log(`hsl(${h}, ${s * 100}, ${l * 100})`)
                ctx.fillRect(x * cellWidth, y * cellHeight, cellWidth, cellHeight)
            })
        })

        const imageUrl = canvas.toDataURL("image/png");
        window.open(imageUrl)
        URL.revokeObjectURL(imageUrl)
    }

    private _exportJSON(paletteData: PaletteModel) {
        const finalData = {
            name: paletteData.name,
            colors: paletteData.colors.flatMap(el => {
                return el.shades.map(el2 => {
                    const hsl = [el.hue, el.saturation, el2];
                    const rgb = this.utils.hslToRgb(hsl[0], hsl[1], hsl[2])
                    const code = this.utils.rgbToHex(rgb[0], rgb[1], rgb[2])

                    return {
                        hue: hsl[0],
                        saturation: hsl[1],
                        light: hsl[2],
                        red: rgb[0],
                        green: rgb[1],
                        blue: rgb[2],
                        code
                    }
                })
            })
        }

        this.utils.exportJsonAsText(finalData, 'colors.json')
    }
}
